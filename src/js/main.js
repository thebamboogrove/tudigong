import * as d3 from 'd3';
import { ckmeans } from 'simple-statistics';
import { Deck, OrthographicView, COORDINATE_SYSTEM } from '@deck.gl/core';
import { GeoJsonLayer, PathLayer } from '@deck.gl/layers';
import { PathStyleExtension } from '@deck.gl/extensions';
import { feature as topojsonFeature, mesh as topojsonMesh } from 'topojson-client';

const sanitizeText = (value) => String(value ?? '').replace(/[\uFEFF\u200B\u202F\u00A0\u2000-\u200A\u2028\u2029]/g, '').trim();

class BoundaryManager {
    constructor() {
        Object.assign(this, this.#initState());
    }

    #initState() {
        return {
            boundaries: null,
            features: null,
            featureIndex: new Map(),
            bounds: null,
            countryFeatures: null,
            extraPaths: null,
            tendashPaths: null
        };
    }

    async loadBoundaries() {
        const topology = await this.#loadTopology();
        this.boundaries = topology;

        const countyKey = this.#findObjectKey(topology, 'county');
        const provinceKey = this.#findObjectKey(topology, 'province');
        const countryKey = this.#findObjectKey(topology, 'country');
        const extraKey = this.#findObjectKey(topology, 'extra');
        const tendashKey = this.#findObjectKey(topology, 'tendash');

        this.features = topojsonFeature(topology, topology.objects[countyKey]).features;
        this.countryFeatures = topojsonFeature(topology, topology.objects[countryKey]).features;
        this.extraFeature = topojsonFeature(topology, topology.objects[extraKey]).features;
        this.tendashFeature = topojsonFeature(topology, topology.objects[tendashKey]).features;

        this.countyMeshGeom = topojsonMesh(topology, topology.objects[countyKey], (a, b) => a !== b);
        this.provinceMeshGeom = topojsonMesh(topology, topology.objects[provinceKey], (a, b) => a !== b);

        const cleanKey = k => sanitizeText(k);

        const sanitizeProps = (props) => {
            if (!props || typeof props !== 'object') return {};
            const out = {};
            for (const key of Object.keys(props)) out[cleanKey(key)] = props[key];
            return out;
        };

        this.normalizeCoordinatesIfNeeded(this.features);
        this.normalizeCoordinatesIfNeeded(this.countryFeatures);
        this.bounds = this.computeBounds(this.features);

        this.features.forEach((feature, index) => {
            feature.properties = sanitizeProps(feature.properties || {});
            const props = feature.properties;
            const normalizeId = v => sanitizeText(v);
            const id = normalizeId(props.CODE ?? props.code ?? `feature_${index}`);
            feature.id = id || `feature_${index}`;
            this.featureIndex.set(feature.id, feature);
        });

        this.countyPaths = this.#meshToPaths(this.countyMeshGeom);
        this.provincePaths = this.#meshToPaths(this.provinceMeshGeom);
        this.extraPaths = this.#featuresToPaths(this.extraFeature);
        this.tendashPaths = this.#featuresToPaths(this.tendashFeature);

        return this.features;
    }

    async #loadTopology() {
        const gzipUrl = './src/boundaries/counties-geo.topojson.gz';
        const jsonUrl = './src/boundaries/counties-geo.topojson';
        if (typeof DecompressionStream === 'function') {
            try {
                return await this.#loadGzipJson(gzipUrl);
            } catch (err) {
                console.warn('gzip fail, plain TopoJSON fallback', err);
            }
        }
        return await this.#loadJson(jsonUrl);
    }

    async #loadGzipJson(url) {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const encoding = response.headers.get('content-encoding') || '';
        if (encoding.toLowerCase().includes('gzip')) {
            return await response.json();
        }
        if (typeof DecompressionStream !== 'function') {
            throw new Error('DecompressionStream null');
        }
        if (!response.body) throw new Error('gzip response body fail');
        const stream = response.body.pipeThrough(new DecompressionStream('gzip'));
        const text = await new Response(stream).text();
        return JSON.parse(text);
    }

    async #loadJson(url) {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json();
    }

    #meshToPaths(geom) {
        if (!geom) return [];
        if (geom.type === 'LineString') return [geom.coordinates];
        if (geom.type === 'MultiLineString') return geom.coordinates;
        return [];
    }

    #featuresToPaths(features) {
        const out = [];
        for (const f of features || []) {
            const g = f?.geometry;
            if (!g) continue;
            if (g.type === 'LineString') out.push(g.coordinates);
            else if (g.type === 'MultiLineString') out.push(...g.coordinates);
        }
        return out;
    }

    #findObjectKey(topology, nameLike) {
        const keys = Object.keys(topology.objects || {});
        const lower = String(nameLike).toLowerCase();
        let key = keys.find(k => k.toLowerCase() === lower) || keys.find(k => k.toLowerCase().includes(lower));
        if (!key) throw new Error(`object "${nameLike}" not found`);
        return key;
    }

    computeBounds(features) {
        let minLon = Infinity, minLat = Infinity, maxLon = -Infinity, maxLat = -Infinity;
        const update = (coord) => {
            const [lon, lat] = coord;
            if (!Number.isFinite(lon) || !Number.isFinite(lat)) return;
            if (lon < minLon) minLon = lon;
            if (lat < minLat) minLat = lat;
            if (lon > maxLon) maxLon = lon;
            if (lat > maxLat) maxLat = lat;
        };
        for (const f of features) {
            const g = f.geometry;
            if (!g) continue;
            if (g.type === 'Polygon') {
                g.coordinates.forEach(ring => ring.forEach(update));
            } else if (g.type === 'MultiPolygon') {
                g.coordinates.forEach(poly => poly.forEach(ring => ring.forEach(update)));
            }
        }
        return [minLon, minLat, maxLon, maxLat];
    }

    normalizeCoordinatesIfNeeded(features) {
        let sample = [];
        for (const f of features) {
            const g = f.geometry;
            if (g.type === 'Polygon') sample.push(g.coordinates[0][0]);
            else if (g.type === 'MultiPolygon') sample.push(g.coordinates[0][0][0]);
            if (sample.length >= 50) break;
        }
        const suspect = sample.filter(c => Array.isArray(c) && Math.abs(c[1]) > 90 && Math.abs(c[0]) <= 180);
        if (suspect.length < Math.ceil(sample.length * 0.5)) return;

        const swap = (coord) => [coord[1], coord[0]];
        for (const f of features) {
            const g = f.geometry;
            if (g.type === 'Polygon') {
                g.coordinates = g.coordinates.map(ring => ring.map(swap));
            } else if (g.type === 'MultiPolygon') {
                g.coordinates = g.coordinates.map(poly => poly.map(ring => ring.map(swap)));
            }
        }
    }
}

class DataManager {
    constructor() {
        Object.assign(this, this.#initState());
    }

    #initState() {
        return {
            cache: new Map(),
            indexes: new Map(),
            loadingPromises: new Map(),
            compositeCache: new Map(),
            packCache: new Map()
        };
    }

    async initialize() {
        const categoryIndex = await this.loadJSON('./src/data/index/category.json');
        this.indexes.set('category', categoryIndex);
        return true;
    }

    async loadJSON(url) {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json();
    }

    async loadBinary(url) {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.arrayBuffer();
    }

    async loadCategoryData(category, forceReload = false) {
        const cacheKey = `category_${category}`;
        if (!forceReload && this.cache.has(cacheKey)) return this.cache.get(cacheKey);
        if (this.loadingPromises.has(cacheKey)) return this.loadingPromises.get(cacheKey);

        const loadPromise = this.loadCategoryDataInternal(category);
        this.loadingPromises.set(cacheKey, loadPromise);
        try {
            const data = await loadPromise;
            this.cache.set(cacheKey, data);
            return data;
        } finally {
            this.loadingPromises.delete(cacheKey);
        }
    }

    async loadCategoryDataInternal(category) {
        const base = `./src/data/metrics/${category}`;
        const index = await this.loadJSON(`${base}/index.json`);

        const idsPayload = await this.loadJSON(this.resolveDataPath(base, index.idsFile || 'ids.json'));
        const rawIds = Array.isArray(idsPayload?.ids) ? idsPayload.ids : [];
        const ids = rawIds.map(id => this.normalizeId(id));

        let dictionaries = {};
        if (index.stringsFile) {
            const stringsPayload = await this.loadJSON(this.resolveDataPath(base, index.stringsFile));
            dictionaries = stringsPayload?.dictionaries || {};
        }

        let names = null;
        if (index.namesFile) {
            const namesPayload = await this.loadJSON(this.resolveDataPath(base, index.namesFile));
            names = Array.isArray(namesPayload?.names) ? namesPayload.names : null;
        }

        const count = Number(index.count ?? ids.length ?? 0);
        const idIndex = new Map();
        ids.forEach((id, i) => idIndex.set(id, i));

        return {
            ids,
            idIndex,
            data: {},
            stringIndex: {},
            stringDicts: {},
            stringDictionaries: dictionaries,
            names,
            metadata: {
                count,
                category,
                numericProperties: index.numericProperties || [],
                stringProperties: index.stringProperties || []
            },
            metricIndex: index
        };
    }

    getCategoryConfig(category) {
        const index = this.indexes.get('category') || {};
        return index[category];
    }

    async ensureMetricsLoaded(category, metricIds, dataOverride = null) {
        if (!metricIds || !metricIds.length) return;
        const data = dataOverride || this.cache.get(`category_${category}`);
        if (!data) return;

        const metricIndex = data.metricIndex?.metrics || {};
        const packs = data.metricIndex?.packs || {};
        const base = `./src/data/metrics/${category}`;

        const pending = new Map();
        metricIds.forEach(metricId => {
            if (!metricId || metricId.startsWith('COMPOSITE__')) return;
            if (data.data[metricId] || data.stringIndex[metricId]) return;

            const meta = metricIndex[metricId];
            if (!meta) {
                console.warn(`Missing metric in index: ${metricId}`);
                return;
            }
            const pack = meta.pack;
            if (!pending.has(pack)) pending.set(pack, []);
            pending.get(pack).push({ metricId, meta });
        });

        for (const [packName, metrics] of pending.entries()) {
            const packMeta = packs[packName];
            if (!packMeta?.file) continue;
            const cacheKey = `${category}:${packName}`;
            let buffer = this.packCache.get(cacheKey);
            if (!buffer) {
                buffer = await this.loadBinary(this.resolveDataPath(base, packMeta.file));
                this.packCache.set(cacheKey, buffer);
            }
            metrics.forEach(entry => this.hydrateMetricFromPack(data, entry.metricId, entry.meta, buffer));
        }
    }

    hydrateMetricFromPack(data, metricId, meta, buffer) {
        const offset = Number(meta.offset || 0);
        const length = Number(meta.length || data.metadata?.count || 0);
        if (!Number.isFinite(offset) || !Number.isFinite(length)) return;

        const dtype = String(meta.dtype || '').toLowerCase();
        let arr = null;

        if (dtype === 'float32') arr = new Float32Array(buffer, offset, length);
        else if (dtype === 'uint16') arr = new Uint16Array(buffer, offset, length);
        else if (dtype === 'uint32') arr = new Uint32Array(buffer, offset, length);

        if (!arr) return;

        if (meta.kind === 'string' || meta.kind === 'categorical') {
            data.stringIndex[metricId] = arr;
            const dictKey = meta.dict || metricId;
            data.stringDicts[metricId] = data.stringDictionaries?.[dictKey] || [];
        } else {
            data.data[metricId] = arr;
        }
    }

    getCompositeKey(category, metricId, parts) {
        const joined = (Array.isArray(parts) && parts.length)
            ? parts.join('__')
            : '__none__';
        return `COMPOSITE__${category}__${metricId}__${joined}`;
    }

    getCompositeBuffer(category, metricId, compositeDef, parts, data) {
        if (!data) return null;
        const definition = compositeDef || null;
        if (!definition?.parts?.length) return null;

        const allowed = Array.isArray(definition.parts) ? definition.parts : [];
        const hasCustomSelection = Array.isArray(parts);
        const requested = hasCustomSelection
            ? parts
            : (definition.default && definition.default.length ? definition.default : allowed);

        const requestedSet = new Set((requested || []).map(p => String(p)));
        const normalizedParts = [];
        allowed.forEach(part => {
            if (requestedSet.has(part)) normalizedParts.push(part);
        });

        const cacheKey = this.getCompositeKey(category, metricId, normalizedParts);
        if (this.compositeCache.has(cacheKey)) {
            const buffer = this.compositeCache.get(cacheKey);
            return { key: cacheKey, buffer };
        }

        const count = data.metadata?.count ?? data.ids?.length ?? 0;
        const buffer = new Float32Array(count);
        if (normalizedParts.length) {
            normalizedParts.forEach(part => {
                const column = data.data[part];
                if (!column) return;
                for (let i = 0; i < count; i += 1) buffer[i] += column[i] || 0;
            });
        }
        this.compositeCache.set(cacheKey, buffer);
        return { key: cacheKey, buffer };
    }

    resolveDataPath(base, file) {
        if (!file) return base;
        if (file.startsWith('http://') || file.startsWith('https://')) return file;
        if (file.startsWith('/')) return file;
        return `${base}/${file}`;
    }

    normalizeId(value) {
        return sanitizeText(value);
    }

    getFeatureValue(data, featureId, property) {
        if (!data || !data.idIndex) return null;
        const index = data.idIndex.get(featureId);
        if (index === undefined) return null;

        if (property && property.startsWith('COMPOSITE__')) {
            const buf = this.compositeCache.get(property);
            return buf ? buf[index] : null;
        }
        if (data.data[property] !== undefined) return data.data[property][index];
        if (data.stringIndex[property] !== undefined) {
            const dict = data.stringDicts[property] || [];
            return dict[data.stringIndex[property][index]] ?? '';
        }
        return null;
    }

    getFeatureValueByFeature(data, feature, property) {
        if (!data || !data.idIndex || !feature) return null;

        const props = feature.properties || {};
        const id = this.normalizeId(props.CODE ?? props.code ?? feature.id);
        const idx = data.idIndex.get(id);
        if (idx !== undefined) {
            if (property && property.startsWith('COMPOSITE__')) {
                const buf = this.compositeCache.get(property);
                if (buf) return buf[idx];
            }
            if (data.data[property]) return data.data[property][idx];
            if (data.stringIndex[property]) {
                const dict = data.stringDicts[property] || [];
                return dict[data.stringIndex[property][idx]] ?? '';
            }
        }
        return null;
    }

    getFeatureNameByFeature(data, feature) {
        if (!data || !data.idIndex || !Array.isArray(data.names) || !feature) return null;
        const props = feature.properties || {};
        const id = this.normalizeId(props.CODE ?? props.code ?? feature.id);
        const idx = data.idIndex.get(id);
        if (idx === undefined) return null;
        const rawName = data.names[idx];
        if (rawName == null) return null;
        const name = String(rawName).trim();
        return name ? name : null;
    }

    getPropertyStats(data, property) {
        if (!data) return null;

        if (property && property.startsWith('COMPOSITE__')) {
            const arr = this.compositeCache.get(property);
            if (!arr) return null;
            const values = Array.from(arr).sort((a, b) => a - b);
            return this.computeNumericStats(values);
        }

        const meta = data.metricIndex?.metrics?.[property];
        if (meta?.stats && meta.kind === 'numeric') {
            return { type: 'numeric', ...meta.stats };
        }

        if (data.data[property]) {
            const values = Array.from(data.data[property]).sort((a, b) => a - b);
            return this.computeNumericStats(values);
        }

        if (data.stringIndex[property]) {
            const dict = data.stringDicts[property] || [];
            const counts = new Map();
            const indices = data.stringIndex[property];
            for (let i = 0; i < indices.length; i += 1) {
                const value = dict[indices[i]] ?? '';
                counts.set(value, (counts.get(value) || 0) + 1);
            }
            const categories = Array.from(counts.entries())
                .sort((a, b) => b[1] - a[1])
                .map(([value, count]) => ({ value, count }));
            return { type: 'categorical', categories, uniqueValues: categories.length };
        }

        if (data.stringData && data.stringData[property]) {
            const counts = new Map();
            data.stringData[property].forEach(v => counts.set(v, (counts.get(v) || 0) + 1));
            const categories = Array.from(counts.entries()).sort((a, b) => b[1] - a[1])
                .map(([value, count]) => ({ value, count }));
            return { type: 'categorical', categories, uniqueValues: categories.length };
        }
        return null;
    }

    computeNumericStats(sortedValues) {
        return {
            type: 'numeric',
            min: sortedValues[0],
            max: sortedValues[sortedValues.length - 1],
            median: sortedValues[Math.floor(sortedValues.length / 2)],
            mean: sortedValues.reduce((a, b) => a + b, 0) / sortedValues.length,
            count: sortedValues.length
        };
    }
}


const AXIS_TEMPLATE_ID = 'axis-svg-template';

const buildAxisSvgFromTemplate = (container, { svgClass = '', labelClass = '' } = {}) => {
    if (!container) return null;
    const tpl = document.getElementById(AXIS_TEMPLATE_ID);
    if (!tpl) return null;

    const frag = tpl.content.cloneNode(true);
    const svg = frag.querySelector('svg');
    const ticksGroup = frag.querySelector('.axis-ticks');
    const labelMin = frag.querySelector('.axis-label-min');
    const labelMax = frag.querySelector('.axis-label-max');
    const labelMid = frag.querySelector('.axis-label-mid');

    if (!svg || !ticksGroup || !labelMin || !labelMax || !labelMid) return null;

    if (svgClass) svg.classList.add(svgClass);
    if (labelClass) [labelMin, labelMax, labelMid].forEach(el => el.classList.add(labelClass));

    container.replaceChildren(frag);

    return {
        svg,
        ticksGroup,
        labelMin,
        labelMax,
        labelMid
    };
};

const SVG_NS = 'http://www.w3.org/2000/svg';

const createSvgLine = (x1, y1, x2, y2, className) => {
    const line = document.createElementNS(SVG_NS, 'line');
    if (className) line.setAttribute('class', className);
    line.setAttribute('x1', `${x1}`);
    line.setAttribute('y1', `${y1}`);
    line.setAttribute('x2', `${x2}`);
    line.setAttribute('y2', `${y2}`);
    line.setAttribute('vector-effect', 'non-scaling-stroke');
    return line;
};

const percentFromT = (t, flip) => {
    const tt = Number(t);
    if (!Number.isFinite(tt)) return null;
    const clamped = Math.max(0, Math.min(1, tt));
    const posT = flip ? 1 - clamped : clamped;
    return `${posT * 100}%`;
};

const toPx = (val, spanPx) => {
    if (typeof val === 'string' && val.endsWith('%')) {
        const pct = Number.parseFloat(val);
        if (Number.isFinite(pct)) return (pct / 100) * spanPx;
    }
    return Number(val);
};

const normalizeAxisLabelSpec = (spec, fallbackT) => {
    if (spec == null) return null;
    if (typeof spec === 'string' || typeof spec === 'number') {
        return { text: String(spec), t: fallbackT };
    }
    if (typeof spec === 'object') {
        const text = spec.text ?? spec.label ?? spec.value ?? '';
        if (text === '' || text == null) return null;
        const t = Number.isFinite(spec.t) ? spec.t : fallbackT;
        return { text: String(text), t, rotate: spec.rotate, anchor: spec.anchor, baseline: spec.baseline };
    }
    return null;
};

const applyAxisLabel = (labelEl, spec, fallbackT, posX, posY, context = {}) => {
    if (!labelEl) return;
    const {
        orientation,
        labelAnchor,
        labelBaseline,
        labelRotation,
        axisW,
        axisH,
        toPct
    } = context;
    const normalized = normalizeAxisLabelSpec(spec, fallbackT);
    if (!normalized) {
        labelEl.style.display = 'none';
        labelEl.textContent = '';
        labelEl.removeAttribute('transform');
        return;
    }

    const posT = typeof toPct === 'function' ? toPct(normalized.t) : null;
    if (posT == null) {
        labelEl.style.display = 'none';
        labelEl.textContent = '';
        labelEl.removeAttribute('transform');
        return;
    }

    const x = orientation === 'x' ? posT : posX;
    const y = orientation === 'x' ? posY : posT;

    labelEl.style.display = '';
    labelEl.textContent = normalized.text;
    labelEl.setAttribute('x', `${x}`);
    labelEl.setAttribute('y', `${y}`);

    const anchor = normalized.anchor ?? labelAnchor;
    if (anchor) labelEl.setAttribute('text-anchor', anchor);

    const baseline = normalized.baseline ?? labelBaseline;
    if (baseline) labelEl.setAttribute('dominant-baseline', baseline);

    const rotate = Number.isFinite(normalized.rotate) ? normalized.rotate : labelRotation;
    if (Number.isFinite(rotate) && rotate !== 0) {
        const xPx = toPx(x, axisW);
        const yPx = toPx(y, axisH);
        if (Number.isFinite(xPx) && Number.isFinite(yPx)) {
            labelEl.setAttribute('transform', `rotate(${rotate} ${xPx} ${yPx})`);
        } else {
            labelEl.removeAttribute('transform');
        }
    } else {
        labelEl.removeAttribute('transform');
    }
};

const renderSvgAxis = ({
    axis,
    width,
    height,
    ticksT = [],
    labels = {},
    orientation = 'x',
    baseOffset = 2,
    tickLen = 6,
    tickDir = 1,
    labelPad = 2,
    flip = false,
    drawAxisLine = true,
    axisLineClass = '',
    tickClass = '',
    labelAnchor = null,
    labelBaseline = null,
    labelRotation = 0
} = {}) => {
    if (!axis?.svg || !axis?.ticksGroup) return;

    const axisW = Math.max(1, Number(width) || 1);
    const axisH = Math.max(1, Number(height) || 1);

    axis.svg.removeAttribute('viewBox');
    axis.svg.setAttribute('width', '100%');
    axis.svg.setAttribute('height', '100%');
    axis.svg.setAttribute('shape-rendering', 'crispEdges');

    axis.ticksGroup.replaceChildren();

    const base = Math.round(baseOffset);
    const tickLenPx = Math.round(tickLen);
    const dir = tickDir >= 0 ? 1 : -1;
    const toPct = (t) => percentFromT(t, flip);

    const labelContext = {
        orientation,
        labelAnchor,
        labelBaseline,
        labelRotation,
        axisW,
        axisH,
        toPct
    };
    const applyLabel = (labelEl, spec, fallbackT, posX, posY) => {
        applyAxisLabel(labelEl, spec, fallbackT, posX, posY, labelContext);
    };

    if (orientation === 'x') {
        if (drawAxisLine) {
            axis.ticksGroup.appendChild(createSvgLine('0%', base, '100%', base, axisLineClass));
        }
        ticksT.forEach((t) => {
            const x = toPct(t);
            if (x == null) return;
            axis.ticksGroup.appendChild(createSvgLine(x, base, x, base + tickLenPx * dir, tickClass));
        });
        const labelY = Math.round(base + tickLenPx * dir + labelPad * dir);
        applyLabel(axis.labelMin, labels.min, 0, '0%', labelY);
        applyLabel(axis.labelMax, labels.max, 1, '100%', labelY);
        applyLabel(axis.labelMid, labels.mid, 0.5, '50%', labelY);
        return;
    }

    if (drawAxisLine) {
        axis.ticksGroup.appendChild(createSvgLine(base, '0%', base, '100%', axisLineClass));
    }
    ticksT.forEach((t) => {
        const y = toPct(t);
        if (y == null) return;
        axis.ticksGroup.appendChild(createSvgLine(base, y, base + tickLenPx * dir, y, tickClass));
    });
    const labelX = Math.round(base + tickLenPx * dir + labelPad * dir);
    applyLabel(axis.labelMin, labels.min, 0, labelX, '0%');
    applyLabel(axis.labelMax, labels.max, 1, labelX, '100%');
    applyLabel(axis.labelMid, labels.mid, 0.5, labelX, '50%');
};

const formatEdgeLabel = ({ value, edge, hasBelow, hasAbove, formatter } = {}) => {
    const formatFn = typeof formatter === 'function' ? formatter : v => String(v ?? '');
    const text = formatFn(value);
    if (edge === 'min' && hasBelow) return `≤ ${text}`;
    if (edge === 'max' && hasAbove) return `≥ ${text}`;
    return text;
};

const formatBinnedRangeLabel = ({ index, count, left, right } = {}) => {
    if (index === 0) return `≤ ${right}`;
    if (index === count - 1) return `≥ ${left}`;
    return `${left} - ${right}`;
};

const computeLinearTicks = ({ min, max, steps, toT } = {}) => {
    const count = Math.max(2, Number(steps) || 2);
    const dmin = Number(min);
    const dmax = Number(max);
    if (!Number.isFinite(dmin) || !Number.isFinite(dmax)) {
        return { ticks: [], ticksT: [] };
    }
    const ticks = d3.range(count).map(i => dmin + (i / (count - 1)) * (dmax - dmin));
    ticks[0] = dmin;
    ticks[ticks.length - 1] = dmax;
    const ticksClean = ticks.map(Number).filter(Number.isFinite);
    const ticksT = typeof toT === 'function'
        ? ticksClean.map(v => Number(toT(v))).filter(t => Number.isFinite(t) && t >= 0 && t <= 1)
        : [];
    return { ticks: ticksClean, ticksT };
};

const getMidTickLabel = ({ ticks, steps, toT, formatter } = {}) => {
    if (!Array.isArray(ticks) || !ticks.length) return null;
    if (ticks.length > 3 || ticks.length !== steps) return null;
    const midIdx = Math.floor(ticks.length / 2);
    if (midIdx <= 0 || midIdx >= ticks.length - 1) return null;
    const tMid = typeof toT === 'function' ? Number(toT(ticks[midIdx])) : NaN;
    if (!Number.isFinite(tMid) || tMid < 0 || tMid > 1) return null;
    const formatFn = typeof formatter === 'function' ? formatter : v => String(v ?? '');
    return { t: tMid, text: formatFn(ticks[midIdx]) };
};

const getDomainFlags = (stats, domain) => {
    const rawMin = Array.isArray(domain) ? Number(domain[0]) : Number(domain);
    const rawMax = Array.isArray(domain) ? Number(domain[domain.length - 1]) : Number(domain);
    let min = Number.isFinite(rawMin) ? rawMin : Number(stats?.min);
    let max = Number.isFinite(rawMax) ? rawMax : Number(stats?.max);
    if (!Number.isFinite(min)) min = Number(stats?.min);
    if (!Number.isFinite(max)) max = Number(stats?.max);
    if (min > max) [min, max] = [max, min];

    const eps = (Number.isFinite(stats?.max) && Number.isFinite(stats?.min))
        ? Math.max(1e-12, (stats.max - stats.min) * 1e-9)
        : 1e-12;
    const hasBelow = Number.isFinite(stats?.min) && (stats.min < min - eps);
    const hasAbove = Number.isFinite(stats?.max) && (stats.max > max + eps);

    return { min, max, hasBelow, hasAbove };
};

const resolveLegendHeader = (dataState, fallbackTitle = '') => ({
    title: dataState?.currentMetricLabel || dataState?.currentMetricId || fallbackTitle,
    description: dataState?.currentMetricDescription || ''
});

const buildLegendHeader = (dataState, fallbackTitle = '') => {
    const { title, description } = resolveLegendHeader(dataState, fallbackTitle);
    const head = document.createElement('div');
    const titleEl = document.createElement('div');
    titleEl.className = 'legend-title';
    titleEl.textContent = title || '';
    head.appendChild(titleEl);

    if (description) {
        const descEl = document.createElement('div');
        descEl.className = 'legend-description';
        descEl.textContent = description;
        head.appendChild(descEl);
    }

    return head;
};

const buildLegendList = (rows = [], badgeText = '') => {
    const list = document.createElement('div');
    list.className = 'legend-categorical';

    rows.forEach(({ color, label }) => {
        const row = document.createElement('div');
        row.className = 'legend-row';

        const swatch = document.createElement('span');
        swatch.className = 'legend-swatch';
        swatch.style.background = color || '#cccccc';

        const text = document.createElement('span');
        text.textContent = label || '';

        row.appendChild(swatch);
        row.appendChild(text);
        list.appendChild(row);
    });

    if (badgeText) {
        const badge = document.createElement('div');
        badge.className = 'legend-badge';
        badge.textContent = badgeText;
        list.appendChild(badge);
    }

    return list;
};

const formatScaleLabel = ({ settings, scaleObj, fallback = 'linear' } = {}) => {
    const scaleName = String(settings?.scale || fallback || 'linear').toLowerCase();
    if (scaleName === 'pow') {
        const k = scaleObj?.__exponentRaw ?? scaleObj?.__exponent ?? settings?.exponent ?? 2;
        return `power scale (k=${k})`;
    }
    return `${scaleName} scale`;
};


class MapRenderer {
    constructor(containerId) {
        this.containerId = containerId;
        this.deckgl = null;

        this.dataState = this.#initDataState();
        this.bivarState = this.#initBivarState();
        this.tooltipState = this.#initTooltipState();
        this.filterState = this.#initFilterState();
        this.cacheState = this.#initCacheState();
        this.legendState = this.#initLegendState();
        this.layerState = this.#initLayerState();
        this.hoverState = this.#initHoverState();
        this.viewState = this.#initViewState();
        this.inputState = this.#initInputState();

        this.#setupTooltipElement();
    }

    #initDataState() {
        return {
            currentData: null,
            currentMetric: null,
            currentMetricId: null,
            currentMetricLabel: null,
            currentMetricDescription: null,
            currentMetricSettings: null
        };
    }

    #initBivarState() {
        return {
            bivar: null,
            valuesX: null,
            valuesY: null,
            data: null,
            cfg: null
        };
    }

    #initTooltipState() {
        return {
            el: document.getElementById('tooltip'),
            contentKey: '',
            size: { w: 0, h: 0 },
            raf: 0,
            nextPos: null,
            showDelay: 80,
            showTimer: 0,
            pending: null,
            visible: false,
            changeDelay: 60,
            changeTimer: 0,
            pendingChange: null,
            pendingKey: '',
            suppressUntil: 0,
            suppressKey: ''
        };
    }

    #initFilterState() {
        return {
            range: null,
            categorical: new Set(),
            bivar: { x: null, y: null }
        };
    }

    #initCacheState() {
        return {
            intl: new Map()
        };
    }

    #initLegendState() {
        return {
            cleanup: null,
            chromeCleanup: null,
            collapsedManual: null,
            collapsed: false,
            cache: null
        };
    }

    #initLayerState() {
        return {
            main: [],
            base: null,
            border: null
        };
    }

    #initHoverState() {
        return {
            feature: null,
            featureId: null
        };
    }

    #initViewState() {
        return {
            currentZoom: null,
            borderWidthCache: null
        };
    }

    #initInputState() {
        return {
            mode: this.isTouchInput() ? 'touch' : 'mouse',
            lastSwitch: 0,
            cleanup: null
        };
    }

    #setupTooltipElement() {
        const tooltip = this.tooltipState.el;
        if (!tooltip) return;
        tooltip.style.left = '0px';
        tooltip.style.top = '0px';
        tooltip.style.transform = 'translate3d(0, 0, 0)';
    }

    getDpr() {
        return window.devicePixelRatio || 1;
    }

    getCanvasDpr(dprCap = 2) {
        return Math.min(dprCap, this.getDpr());
    }

    getBorderWidths(zoom = null) {
        const baseZoom = Number.isFinite(this.baseZoom) ? this.baseZoom : 0;
        const z = Number.isFinite(zoom)
            ? zoom
            : (Number.isFinite(this.viewState.currentZoom) ? this.viewState.currentZoom : baseZoom);
        const span = 10;
        const tRaw = span > 0 ? (z - baseZoom) / span : 0;
        const t = Math.max(0, Math.min(1, tRaw));
        const countyMin = 0.4;
        const countyMax = 1.4;
        const provinceMin = 0.6;
        const provinceMax = 3;
        const widthBoundary = (a, b) => a + (b - a) * t;
        return {
            county: widthBoundary(countyMin, countyMax),
            province: widthBoundary(provinceMin, provinceMax)
        };
    }

    updateBorderWidths(zoom) {
        const z = Number.isFinite(zoom)
            ? zoom
            : (Number.isFinite(this.viewState.currentZoom) ? this.viewState.currentZoom : this.baseZoom);
        const widths = this.getBorderWidths(z);
        const rounded = {
            county: Math.round(widths.county * 100) / 100,
            province: Math.round(widths.province * 100) / 100
        };
        const prev = this.viewState.borderWidthCache;
        if (prev && prev.county === rounded.county && prev.province === rounded.province) {
            this.viewState.currentZoom = z;
            return;
        }
        this.viewState.currentZoom = z;
        this.viewState.borderWidthCache = rounded;
        this.layerState.border = null;
        if (this.deckgl) this.refreshLayers();
    }

    debounce(fn, ms = 150) {
        let t = null;
        return (...args) => {
            if (t) clearTimeout(t);
            t = setTimeout(() => fn(...args), ms);
        };
    }

    isMobileLegend() {
        return window.matchMedia?.('(max-width: 640px)')?.matches ?? (window.innerWidth <= 640);
    }

    hasCollapsibleLegend() {
        const content = document.getElementById('legendContent');
        const body = content?.querySelector('.legend-body');
        return !!body && body.childNodes.length > 0;
    }

    wrapLegendForToggle(headEl, bodyEl) {
        const shell = document.createElement('div');
        shell.className = 'legend-shell';

        if (headEl) {
            headEl.classList.add('legend-head', 'legend-text');
            shell.appendChild(headEl);
        }

        const bodyWrap = document.createElement('div');
        bodyWrap.className = 'legend-body';
        if (bodyEl) bodyWrap.appendChild(bodyEl);
        shell.appendChild(bodyWrap);
        return shell;
    }

    setLegendCollapsed(collapsed) {
        const legend = document.getElementById('legend');
        const btn = document.getElementById('legendToggle');
        if (!legend || !btn) return;
        this.legendState.collapsed = !!collapsed;
        legend.classList.toggle('is-collapsed', this.legendState.collapsed);
        btn.setAttribute('aria-expanded', String(!this.legendState.collapsed));
    }

    syncLegendToggle() {
        const legend = document.getElementById('legend');
        const btn = document.getElementById('legendToggle');
        if (!legend || !btn) return;

        const show = this.hasCollapsibleLegend();
        btn.style.display = show ? 'inline-flex' : 'none';
        if (!show) {
            legend.classList.remove('is-collapsed');
            this.legendState.collapsed = false;
            this.legendState.collapsedManual = null;
            btn.setAttribute('aria-expanded', 'true');
            return;
        }

        if (this.legendState.collapsedManual == null) this.setLegendCollapsed(this.isMobileLegend());
        else this.setLegendCollapsed(this.legendState.collapsedManual);
    }

    initLegendChrome() {
        const legend = document.getElementById('legend');
        const btn = document.getElementById('legendToggle');
        if (!legend || !btn) return;

        const applyAuto = () => {
            if (this.legendState.collapsedManual != null) return;
            this.setLegendCollapsed(this.isMobileLegend());
        };

        btn.onclick = () => {
            this.legendState.collapsedManual = !this.legendState.collapsed;
            this.setLegendCollapsed(this.legendState.collapsedManual);
        };

        const onResize = this.debounce(applyAuto, 180);
        window.addEventListener('resize', onResize, { passive: true });
        this.legendState.chromeCleanup = () => window.removeEventListener('resize', onResize);

        this.setLegendCollapsed(false);
        applyAuto();
        this.syncLegendToggle();
    }

    refreshLegendChrome() {
        this.syncLegendToggle();
    }

    sizeCanvas(canvas, cssW, cssH, { dprCap = 2, style } = {}) {
        const dpr = this.getCanvasDpr(dprCap);
        if (style && typeof style === 'object') Object.assign(canvas.style, style);

        const pxW = Math.max(1, Math.round(cssW * dpr));
        const pxH = Math.max(1, Math.round(cssH * dpr));
        if (canvas.width !== pxW) canvas.width = pxW;
        if (canvas.height !== pxH) canvas.height = pxH;

        const ctx = canvas.getContext('2d');
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

        const crisp = (x) => (Math.round(x * dpr) + 0.5) / dpr;
        return { ctx, dpr, crisp, cssW, cssH, pxW, pxH };
    }

    makeHiDPICanvas(cssW, cssH, { className, style, dprCap = 2 } = {}) {
        const canvas = document.createElement('canvas');
        if (className) canvas.className = className;

        canvas.style.display = 'block';
        canvas.style.width = `${cssW}px`;
        canvas.style.height = `${cssH}px`;
        const pack = this.sizeCanvas(canvas, cssW, cssH, { dprCap, style });
        return { canvas, ...pack };
    }

    ensureGradientLegendCache() {
        if (this.legendState.cache?.type === 'gradient') return this.legendState.cache;

        const tpl = document.getElementById('legend-gradient-template');
        if (!tpl) return null;

        const frag = tpl.content.cloneNode(true);
        const titleEl = frag.querySelector('.legend-title');
        const descEl = frag.querySelector('.legend-description');
        const headEl = frag.querySelector('.legend-text');
        const bodyEl = frag.querySelector('.legend-gradient');
        const canvasWrap = frag.querySelector('.legend-canvas-wrap');
        const axisEl = frag.querySelector('.legend-axis');
        const badgeEl = frag.querySelector('.legend-badge');

        const shell = this.wrapLegendForToggle(headEl, bodyEl);

        const gradCanvas = document.createElement('canvas');
        gradCanvas.style.display = 'block';
        gradCanvas.style.width = '220px';
        gradCanvas.style.height = '12px';
        canvasWrap.replaceChildren(gradCanvas);

        axisEl.style.position = axisEl.style.position || 'relative';

        const axis = buildAxisSvgFromTemplate(axisEl, {
            svgClass: 'legend-axis-svg',
            labelClass: 'legend-axis-label'
        });
        if (!axis) return null;

        this.legendState.cache = {
            type: 'gradient',
            shell,
            titleEl,
            descEl,
            canvasWrap,
            axisEl,
            badgeEl,
            gradCanvas,
            axis
        };
        return this.legendState.cache;
    }


    initialize(options = {}) {
        this.initLegendChrome();
        const container = document.getElementById(this.containerId);
        const initialViewState = options.bounds
            ? this.computeViewFromBounds(options.bounds, container)
            : { target: [0, 0, 0], zoom: 0, pitch: 0, bearing: 0 };
        this.baseZoom = initialViewState.zoom ?? 0;
        this.viewState.currentZoom = initialViewState.zoom ?? 0;
        this.updateBorderWidths(this.viewState.currentZoom);
        console.log('Base zoom: ' + this.baseZoom);

        this.deckgl = new Deck({
            useDevicePixels: true,
            parent: container,
            views: [new OrthographicView({ id: 'ortho', flipY: false })],
            initialViewState,
            controller: true,
            layerFilter: ({layer, viewport}) => {
                if (layer.id === 'borders-county') return viewport.zoom >= (this.baseZoom + 3);
                return true;
            },
            onViewStateChange: ({ viewState }) => {
                this.updateBorderWidths(viewState?.zoom);
            },
            onHover: this.onHover.bind(this),
            onClick: this.onClick.bind(this)
        });
        this.setupInputModeListeners(container);
    }


    buildInterpolator(settings = {}, stats) {
        const interpolation = settings.interpolation || settings || {};
        const cfgType = String(settings?.type || '').toLowerCase();
        const scaleType = String(settings?.scale || '').toLowerCase();
        const mode = interpolation.type || cfgType;
        const isCategorical = stats?.type === 'categorical'
            || cfgType === 'categorical'
            || mode === 'categorical'
            || scaleType === 'ordinal';

        if (isCategorical) {
            const domain = this.resolveCategoricalDomain(stats, settings);
            const palette = this.expandPalette(
                this.resolveCategoricalPalette(interpolation, domain.length || 10),
                domain.length || 1
            );
            const actualDomain = domain.length ? domain : palette.map((_, i) => i);
            const scale = d3.scaleOrdinal(palette).domain(actualDomain).unknown('#cccccc');
            const ordinal = (value) => scale(value == null ? '__missing__' : value);
            ordinal.scale = scale;
            return ordinal;
        }

        const { value, gamma, piecewise } = interpolation;
        const interpType = (mode || 'named').toLowerCase();

        if (interpType === 'named') {
            const fn = d3[`interpolate${value}`] || d3.interpolateViridis;
            return fn;
        }

        if (interpType === 'rgb') {
            const g = gamma > 0 ? gamma : 1;
            const rgbFactory = (d3.interpolateRgb.gamma ? d3.interpolateRgb.gamma(g) : d3.interpolateRgb);
            if (piecewise === true && Array.isArray(value) && value.length >= 2) {
                return d3.piecewise(rgbFactory, value);
            }
            if (Array.isArray(value) && value.length > 2) {
                return d3.interpolateRgbBasis(value);
            }
            return rgbFactory(value?.[0], value?.[1]);
        }

        if (interpType === 'cubehelix') {
            const g = gamma > 0 ? gamma : 1;
            const cubeFactory = (d3.interpolateCubehelix.gamma ? d3.interpolateCubehelix.gamma(g) : d3.interpolateCubehelix);
            if (piecewise === true && Array.isArray(value) && value.length >= 2) {
                return d3.piecewise(cubeFactory, value);
            }
            return cubeFactory(value?.[0], value?.[1]);
        }

        return d3.interpolateViridis;
    }

    resolveBivarPalette(bivar, xSet, ySet) {
        const palette = bivar?.method?.palette;
        if (!Array.isArray(palette) || !palette.length) return null;
        if (!Array.isArray(palette[0])) return null;

        const rows = palette.filter(row => Array.isArray(row) && row.length);
        if (!rows.length) return null;

        const minCols = Math.min(...rows.map(row => row.length));
        const grid = rows.map(row => row.slice(0, minCols));

        const binsXSetting = Math.floor(Number(xSet?.paletteSteps));
        const binsYSetting = Math.floor(Number(ySet?.paletteSteps));
        const binsX = Number.isFinite(binsXSetting) && binsXSetting > 0 ? binsXSetting : minCols;
        const binsY = Number.isFinite(binsYSetting) && binsYSetting > 0 ? binsYSetting : grid.length;

        if (binsX !== minCols || binsY !== grid.length) {
            console.warn('Bivariate palette size mismatch; using palette dimensions.', {
                binsX,
                binsY,
                paletteCols: minCols,
                paletteRows: grid.length
            });
        }

        return { grid, binsX: minCols, binsY: grid.length };
    }

    resolveCategoricalDomain(stats, settings) {
        const domain = Array.isArray(settings?.domain) ? settings.domain.slice() : [];
        if (Array.isArray(stats?.categories)) {
            stats.categories.forEach(cat => {
                if (!domain.includes(cat.value)) domain.push(cat.value);
            });
        }
        return domain;
    }

    resolveCategoricalPalette(interpolation, desiredSize) {
        if (Array.isArray(interpolation?.value) && interpolation.value.length) {
            return interpolation.value.slice();
        }
        if (typeof interpolation?.value === 'string') {
            const name = interpolation.value.startsWith('scheme')
                ? interpolation.value
                : `scheme${interpolation.value.charAt(0).toUpperCase()}${interpolation.value.slice(1)}`;
            const scheme = d3[name];
            if (Array.isArray(scheme)) {
                if (Array.isArray(scheme[0])) {
                    const idx = Math.min(Math.max(desiredSize, 3), scheme.length - 1);
                    return (scheme[desiredSize] || scheme[idx] || []).slice();
                }
                return scheme.slice();
            }
        }
        const fallback = d3.schemeTableau10 || d3.schemeCategory10;
        return fallback.slice ? fallback.slice() : Array.from(fallback);
    }

    expandPalette(palette, size) {
        if (!palette.length) return ['#999999'];
        if (palette.length >= size) return palette.slice(0, size);
        const out = [];
        while (out.length < size) out.push(palette[out.length % palette.length]);
        return out;
    }


    resolveNumericDomain(stats, settings) {
        let dmin, dmax;
        if (Array.isArray(settings?.domain) && settings.domain.length === 2) {
            [dmin, dmax] = settings.domain;
        } else {
            dmin = stats.min;
            dmax = stats.max;
        }
        dmin = Number(dmin);
        dmax = Number(dmax);
        if (dmin > dmax) [dmin, dmax] = [dmax, dmin];
        return { dmin, dmax };
    }

    resolveBinningConfig(settings, stats, fallbackBins, hasPalette = false) {
        const binning = settings?.binning || {};
        let method = String(binning?.method || '').toLowerCase();
        if (!method && hasPalette) method = 'quantize';
        if (!['quantize', 'quantile', 'cluster', 'breakpoints'].includes(method)) return null;

        if (method === 'breakpoints') {
            const breaks = this.resolveBreakpoints(settings, stats);
            if (!breaks || !Number.isFinite(breaks.bins) || breaks.bins < 2) return null;
            return { method, bins: breaks.bins, thresholds: breaks.thresholds, edges: breaks.edges };
        }

        let bins = Math.floor(Number(binning?.bins));
        if (!Number.isFinite(bins) || bins <= 0) bins = Math.floor(Number(fallbackBins));
        if (!Number.isFinite(bins) || bins <= 0) bins = 2;
        bins = Math.max(2, bins);

        return { method, bins };
    }

    resolveBreakpoints(settings, stats) {
        const raw = settings?.binning?.breakpoints;
        if (!Array.isArray(raw)) return null;
        const values = raw.map(Number).filter(Number.isFinite).sort((a, b) => a - b);
        if (!values.length) return null;
        const points = [];
        for (const v of values) {
            if (!points.length || points[points.length - 1] !== v) points.push(v);
        }

        let { dmin, dmax } = this.resolveNumericDomain(stats || {}, settings);
        const hasDomain = Number.isFinite(dmin) && Number.isFinite(dmax);
        if (hasDomain && dmin > dmax) [dmin, dmax] = [dmax, dmin];
        const eps = hasDomain ? Math.max(1e-12, Math.abs(dmax - dmin) * 1e-9) : 1e-12;

        let edges = null;
        let thresholds = null;
        if (!hasDomain && points.length < 2) {
            thresholds = points;
        } else if (hasDomain && Math.abs(points[0] - dmin) <= eps && Math.abs(points[points.length - 1] - dmax) <= eps) {
            edges = points;
            thresholds = points.slice(1, -1);
        } else if (!hasDomain) {
            edges = points;
            thresholds = points.slice(1, -1);
        } else {
            thresholds = points;
            edges = [dmin, ...thresholds, dmax];
        }
        const bins = edges ? edges.length - 1 : thresholds.length + 1;
        return { thresholds, edges, bins };
    }

    buildBinner(values, stats, settings, fallbackBins, hasPalette = false) {
        const cfg = this.resolveBinningConfig(settings, stats, fallbackBins, hasPalette);
        if (!cfg) return null;

        const { method, bins } = cfg;
        const range = d3.range(bins);
        let scale = null;

        if (method === 'quantize') {
            const { dmin, dmax } = this.resolveNumericDomain(stats, settings);
            if (!Number.isFinite(dmin) || !Number.isFinite(dmax)) return null;
            scale = d3.scaleQuantize().domain([dmin, dmax]).range(range);
        } else if (method === 'breakpoints') {
            const thresholds = Array.isArray(cfg.thresholds) ? cfg.thresholds : null;
            if (!thresholds) return null;
            scale = d3.scaleThreshold().domain(thresholds).range(range);
        } else {
            const domain = (values || []).map(Number).filter(Number.isFinite);
            if (!domain.length) return null;

            if (method === 'quantile') {
                scale = d3.scaleQuantile().domain(domain).range(range);
            } else if (method === 'cluster') {
                if (typeof ckmeans === 'function') {
                    const clusters = ckmeans(domain.slice().sort((a, b) => a - b), bins);
                    const thresholds = clusters.map(group => group[group.length - 1]).slice(0, -1);
                    scale = d3.scaleThreshold().domain(thresholds).range(range);
                    scale.__clusters = clusters;
                } else {
                    // quantile fallback
                    scale = d3.scaleQuantile().domain(domain).range(range);
                }
            }
        }

        if (!scale) return null;

        const edges = method === 'breakpoints'
            ? (cfg.edges || this.buildBinnerEdges(scale, method, stats, settings))
            : this.buildBinnerEdges(scale, method, stats, settings);
        const index = (v) => {
            const idx = scale(Number(v));
            if (!Number.isFinite(idx)) return null;
            return Math.max(0, Math.min(bins - 1, idx));
        };
        const t = (v) => {
            const idx = index(v);
            if (idx == null) return 0;
            return (idx + 0.5) / bins;
        };

        return { method, bins, scale, edges, index, t };
    }

    buildBinnerEdges(scale, method, stats, settings) {
        if (method === 'breakpoints') {
            const breaks = this.resolveBreakpoints(settings, stats);
            return breaks?.edges || null;
        }
        const { dmin, dmax } = this.resolveNumericDomain(stats, settings);
        let thresholds = [];
        if (method === 'quantize' && typeof scale.thresholds === 'function') {
            thresholds = scale.thresholds();
        } else if (method === 'quantile' && typeof scale.quantiles === 'function') {
            thresholds = scale.quantiles();
        } else if (method === 'cluster' && Array.isArray(scale.__clusters)) {
            thresholds = scale.__clusters.map(group => group[group.length - 1]).slice(0, -1);
        } else if (typeof scale.quantiles === 'function') {
            thresholds = scale.quantiles();
        }
        thresholds = (thresholds || []).map(Number).filter(Number.isFinite);
        const edges = [dmin, ...thresholds, dmax].map(Number).filter(Number.isFinite);
        return edges.length >= 2 ? edges : null;
    }

    resolveUniPalette(settings, interpolator, bins) {
        const explicit = Array.isArray(settings?.palette) ? settings.palette.slice() : null;
        if (explicit && explicit.length) return this.expandPalette(explicit, bins);
        if (typeof interpolator === 'function') return d3.quantize(interpolator, bins);
        return this.expandPalette([], bins);
    }

    buildScaler(stats, settings) {
        if (stats.type === 'categorical') {
            const passthrough = v => v;
            passthrough.scale = null;
            return passthrough;
        }

        const type = String(settings?.scale || 'linear').toLowerCase();

        let { dmin, dmax } = this.resolveNumericDomain(stats, settings);

        const rmin = 0, rmax = 1;

        const parseExponent = (expRaw) => {
            if (expRaw == null) return undefined;
            if (typeof expRaw === 'number') return expRaw;
            const s = String(expRaw).trim();
            if (/^-?\d+(\.\d+)?$/.test(s)) return parseFloat(s);
            const frac = s.match(/^\s*(-?\d+(?:\.\d+)?)\s*\/\s*(-?\d+(?:\.\d+)?)\s*$/);
            if (frac) {
                const a = parseFloat(frac[1]);
                const b = parseFloat(frac[2]);
                if (b !== 0) return a / b;
            }
            return undefined;
        };

        let scale;
        switch (type) {
            case 'log': {
                const safeMin = Math.max(Number.EPSILON, dmin > 0 ? dmin : Number.EPSILON);
                const safeMax = Math.max(safeMin * (1 + 1e-6), dmax);
                scale = d3.scaleLog().domain([safeMin, safeMax]).range([rmin, rmax]).clamp(true);
                break;
            }
            case 'sqrt': {
                const safeMin = Math.max(0, dmin);
                const safeMax = Math.max(safeMin, dmax);
                scale = d3.scaleSqrt().domain([safeMin, safeMax]).range([rmin, rmax]).clamp(true);
                break;
            }
            case 'pow': {
                const expParsed = parseExponent(settings?.exponent);
                const exp = Number.isFinite(expParsed) ? expParsed : 2;
                const safeMin = Math.max(0, dmin);
                const safeMax = Math.max(safeMin, dmax);
                scale = d3.scalePow().exponent(exp).domain([safeMin, safeMax]).range([rmin, rmax]).clamp(true);
                scale.__exponent = exp;
                scale.__exponentRaw = settings?.exponent;
                break;
            }
            case 'symlog': {
                const c = Number(settings?.constant) || 1;
                scale = d3.scaleSymlog().constant(c).domain([dmin, dmax]).range([rmin, rmax]).clamp(true);
                break;
            }
            case 'linear':
            default: {
                scale = d3.scaleLinear().domain([dmin, dmax]).range([rmin, rmax]).clamp(true);
            }
        }

        if (settings?.nice && typeof scale.nice === 'function') {
            scale.nice();
        }

        const normalize = (v) => scale(Number(v));
        normalize.scale = scale;
        return normalize;
    }


    updateBivariateLayers(features, dataByAxis, bivar, cfgByAxis, filters) {

        this.bivarState.bivar = bivar;
        this.bivarState.data = dataByAxis || null;
        this.bivarState.cfg = cfgByAxis || null;
        if (!this.dataState.currentData) {
            this.dataState.currentData = dataByAxis?.x || dataByAxis?.y || null;
        }
        this.dataState.currentMetricId = null;
        this.dataState.currentMetricLabel = bivar?.label || null;
        this.dataState.currentMetricDescription = bivar?.description || null;
        this.dataState.currentMetricSettings = null;
        this.filterState.bivar = filters || { x: null, y: null };
        this.dataState.currentMetric = null; // clear

        const xData = dataByAxis?.x || this.dataState.currentData;
        const yData = dataByAxis?.y || this.dataState.currentData;
        const key = `${bivar.key}::${bivar?.x?.category || ''}::${bivar?.y?.category || ''}`;
        if (!this.bivarState.valuesX || !this.bivarState.valuesY || this.bivarKey !== key) {
            this.bivarState.valuesX = features.map(f => app.dataManager.getFeatureValueByFeature(xData, f, bivar.x.unit));
            this.bivarState.valuesY = features.map(f => app.dataManager.getFeatureValueByFeature(yData, f, bivar.y.unit));
            this.bivarKey = key;
        }

        const xStats = app.dataManager.getPropertyStats(xData, bivar.x.unit);
        const yStats = app.dataManager.getPropertyStats(yData, bivar.y.unit);
        const xCfg = cfgByAxis?.x || null;
        const yCfg = cfgByAxis?.y || null;
        const xSet = { ...(bivar?.x?.settings || (xCfg?.metrics && xCfg.metrics[bivar.x.metricId]?.settings) || {}) };
        const ySet = { ...(bivar?.y?.settings || (yCfg?.metrics && yCfg.metrics[bivar.y.metricId]?.settings) || {}) };
        if (!xSet.interpolation) xSet.interpolation = { type: 'named', value: 'Blues' };
        if (!ySet.interpolation) ySet.interpolation = { type: 'named', value: 'Oranges' };

        const xNorm = this.buildScaler(xStats, xSet);
        const yNorm = this.buildScaler(yStats, ySet);
        const paletteInfo = this.resolveBivarPalette(bivar, xSet, ySet);
        const paletteKey = paletteInfo ? JSON.stringify(paletteInfo.grid) : null;
        const interpX = paletteInfo ? null : this.buildInterpolator(xSet, xStats);
        const interpY = paletteInfo ? null : this.buildInterpolator(ySet, yStats);
        const defaultXBins = Math.max(2, Number(xSet?.paletteSteps || xSet?.legendSteps || 4));
        const defaultYBins = Math.max(2, Number(ySet?.paletteSteps || ySet?.legendSteps || 4));
        const xBinner = this.buildBinner(this.bivarState.valuesX, xStats, xSet, defaultXBins);
        const yBinner = this.buildBinner(this.bivarState.valuesY, yStats, ySet, defaultYBins);

        if (paletteInfo) {
            if (xBinner && xBinner.bins !== paletteInfo.binsX) {
                console.warn('Bivariate X binning mismatch; using palette columns.', {
                    binsX: xBinner.bins,
                    paletteCols: paletteInfo.binsX
                });
            }
            if (yBinner && yBinner.bins !== paletteInfo.binsY) {
                console.warn('Bivariate Y binning mismatch; using palette rows.', {
                    binsY: yBinner.bins,
                    paletteRows: paletteInfo.binsY
                });
            }
        }

        const blendMode = bivar?.blendMode || 'additive';

        const blend = (cx, cy) => {
            const c1 = d3.rgb(cx), c2 = d3.rgb(cy);
            if (blendMode === 'multiply') {
                return [
                    Math.round((c1.r * c2.r) / 255),
                    Math.round((c1.g * c2.g) / 255),
                    Math.round((c1.b * c2.b) / 255),
                    255
                ];
            } else if (blendMode === 'screen') {
                return [
                    255 - Math.round(((255 - c1.r) * (255 - c2.r)) / 255),
                    255 - Math.round(((255 - c1.g) * (255 - c2.g)) / 255),
                    255 - Math.round(((255 - c1.b) * (255 - c2.b)) / 255),
                    255
                ];
            }
            // default additive
            return [
                Math.max(0, Math.min(255, c1.r + c2.r - 255)),
                Math.max(0, Math.min(255, c1.g + c2.g - 255)),
                Math.max(0, Math.min(255, c1.b + c2.b - 255)),
                255
            ];
        };

        const paletteGrid = paletteInfo ? paletteInfo.grid.map(row => row.map(color => d3.rgb(color))) : null;
        const binsX = paletteInfo ? paletteInfo.binsX : (xBinner?.bins || 0);
        const binsY = paletteInfo ? paletteInfo.binsY : (yBinner?.bins || 0);
        const getPaletteColor = (xIdx, yIdx) => {
            const xClamped = Math.min(binsX - 1, Math.max(0, xIdx));
            const yClamped = Math.min(binsY - 1, Math.max(0, yIdx));
            const row = (binsY - 1) - yClamped;
            const cell = paletteGrid?.[row]?.[xClamped];
            if (!cell) return [200, 200, 200, 255];
            return [cell.r, cell.g, cell.b, 255];
        };

        const layer = new GeoJsonLayer({
            id: 'choropleth-bivar',
            data: features,
            coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
            filled: true,
            stroked: false,
            pickable: true,
            getFillColor: (f, {index}) => {
                const vx = this.bivarState.valuesX[index];
                const vy = this.bivarState.valuesY[index];

                if (!Number.isFinite(vx) || !Number.isFinite(vy)) return [200,200,200,255];

                if (this.filterState.bivar.x && (vx < this.filterState.bivar.x.min || vx > this.filterState.bivar.x.max)) return [0,0,0,0];
                if (this.filterState.bivar.y && (vy < this.filterState.bivar.y.min || vy > this.filterState.bivar.y.max)) return [0,0,0,0];

                const tx = xBinner ? xBinner.t(vx) : xNorm(vx);
                const ty = yBinner ? yBinner.t(vy) : yNorm(vy);

                if (paletteInfo) {
                    const xIdx = xBinner ? xBinner.index(vx) : Math.min(binsX - 1, Math.max(0, Math.floor(tx * binsX)));
                    const yIdx = yBinner ? yBinner.index(vy) : Math.min(binsY - 1, Math.max(0, Math.floor(ty * binsY)));
                    if (xIdx == null || yIdx == null) return [200,200,200,255];
                    return getPaletteColor(xIdx, yIdx);
                }
                return blend(interpX(tx), interpY(ty));
            },
            updateTriggers: {
                getFillColor: [
                    this.bivarKey,
                    this.filterState.bivar?.x?.min, this.filterState.bivar?.x?.max,
                    this.filterState.bivar?.y?.min, this.filterState.bivar?.y?.max,
                    xSet?.scale, xSet?.domain, ySet?.scale, ySet?.domain,
                    xSet?.binning?.method, xSet?.binning?.bins,
                    ySet?.binning?.method, ySet?.binning?.bins,
                    paletteKey,
                    binsX,
                    binsY,
                    blendMode
                ]
            }
        });

        this.renderDeck([layer]);
        this.updateBivariateLegend(bivar, xStats, yStats, xNorm, yNorm, interpX, interpY, xSet, ySet, blendMode, xBinner, yBinner, paletteInfo);
    }

    updateLayers(features, data, metric, metricKey, cfg, filterRange, catFilter) {

        this.dataState.currentData = data;
        this.dataState.currentMetric = metric;
        this.dataState.currentMetricId = metricKey;
        this.dataState.currentMetricLabel = cfg?.metrics?.[metricKey]?.label || metricKey;
        this.dataState.currentMetricDescription = cfg?.metrics?.[metricKey]?.description || null;
        this.dataState.currentMetricSettings = (cfg?.metrics?.[metricKey]?.settings) || null;
        this.bivarState.bivar = null; // clear bivariate
        this.filterState.range = filterRange;
        this.filterState.categorical = catFilter;

        const stats = app.dataManager.getPropertyStats(data, metric);
        if (!stats) {
            return;
        }

        const settings = (cfg.metrics && cfg.metrics[metricKey]?.settings) || { scale: 'linear' };
        const normalize = this.buildScaler(stats, settings);
        const interpolator = this.buildInterpolator(settings, stats);

        const values = features.map(f => app.dataManager.getFeatureValueByFeature(data, f, metric));
        const defaultBins = Math.max(2, Number(settings?.legendSteps || 6));
        const hasPalette = Array.isArray(settings?.palette) && settings.palette.length > 0;
        const binner = this.buildBinner(values, stats, settings, hasPalette ? settings.palette.length : defaultBins, hasPalette);
        const palette = binner ? this.resolveUniPalette(settings, interpolator, binner.bins) : null;

        const layer = new GeoJsonLayer({
            id: 'choropleth',
            data: features,
            coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
            filled: true,
            stroked: false,
            pickable: true,
            getFillColor: (f, {index}) => {
                const v = values[index];
                if (v == null) return [200,200,200,255];

                if (stats.type === 'categorical') {
                    if (this.filterState.categorical && this.filterState.categorical.size > 0 && !this.filterState.categorical.has(v)) return [0,0,0,0];
                    const c = d3.rgb(interpolator(v));
                    return [c.r, c.g, c.b, 255];
                }

                if (!Number.isFinite(v)) return [200,200,200,255];
                if (this.filterState.range && (v < this.filterState.range.min || v > this.filterState.range.max)) return [0,0,0,0];

                if (binner && palette) {
                    const idx = binner.index(v);
                    if (idx == null) return [200,200,200,255];
                    const c = d3.rgb(palette[idx] || palette[palette.length - 1]);
                    return [c.r, c.g, c.b, 255];
                }

                const t = normalize(v); // normalization [0,1]
                const c = d3.rgb(interpolator(t));
                return [c.r, c.g, c.b, 255];
            },
            updateTriggers: {
                getFillColor: [
                    metric,
                    filterRange?.min, filterRange?.max,
                    catFilter,
                    settings?.scale, settings?.domain, settings?.exponent,
                    settings?.binning?.method, settings?.binning?.bins,
                    settings?.palette
                ]
            }
        });

        this.renderDeck([layer]);
        this.updateLegend(stats, interpolator, normalize, settings, binner, palette);
    }

    getFeatureIdFromFeature(feature) {
        const props = feature?.properties || {};
        return app.dataManager.normalizeId(props.CODE ?? props.code ?? feature?.id);
    }

    ensureStaticLayers() {
        if (!this.layerState.base) {
            this.layerState.base = [
                new GeoJsonLayer({
                    id: 'country-fill',
                    data: app.boundaryManager.countryFeatures || [],
                    coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
                    filled: true, stroked: false, pickable: false,
                    getFillColor: [190, 190, 190, 255]
                })
            ];
        }

        if (!this.layerState.border) {
            const borderWidths = this.viewState.borderWidthCache || this.getBorderWidths();
            const countyWidth = borderWidths.county;
            const provinceWidth = borderWidths.province;
            this.layerState.border = [
                new PathLayer({
                    id: 'borders-county',
                    data: app.boundaryManager.countyPaths || [],
                    coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
                    getPath: d => d, getColor: [55, 55, 55, 255],
                    getWidth: countyWidth,
                    widthUnits: "pixels",
                    jointRounded: false,
                    capRounded: false,
                    fp64: false,
                    pickable: false,
                }),
                new PathLayer({
                    id: 'borders-province',
                    data: app.boundaryManager.provincePaths || [],
                    coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
                    getPath: d => d, getColor: [30, 30, 30, 255],
                    getWidth: provinceWidth,
                    widthUnits: "pixels",
                    jointRounded: false,
                    capRounded: false,
                    fp64: false,
                    pickable: false,
                }),
                new PathLayer({
                    id: 'borders-extra',
                    data: app.boundaryManager.extraPaths || [],
                    coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
                    getPath: d => d, getColor: [30, 30, 30, 255],
                    getDashArray: [12, 6],
                    dashJustified: false,
                    dashGapPickable: true,
                    pickable: false,
                    extensions: [new PathStyleExtension({highPrecisionDash: true})],
                    getWidth: provinceWidth,
                    widthUnits: "pixels",
                    jointRounded: true,
                    capRounded: true
                }),
                new PathLayer({
                    id: 'tendash-line',
                    data: app.boundaryManager.tendashPaths || [],
                    coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
                    getPath: d => d, getColor: [30, 30, 30, 255],
                    widthMinPixels: 1
                })
            ];
        }
    }

    buildHighlightLayers() {
        if (!this.hoverState.feature) return null;
        const data = [this.hoverState.feature];

        const shared = {
            data,
            coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
            filled: false,
            stroked: true,
            pickable: false,
            lineCapRounded: true,
            lineJointRounded: true,
            lineWidthUnits: "pixels",
            parameters: { depthTest: false },
        };

        const outlineStroke = new GeoJsonLayer({
            id: 'hover-highlight-outline',
            ...shared,
            getLineColor: [225, 225, 225],
            getLineWidth: 5,
            lineWidthMinPixels: 3,
            lineWidthMaxPixels: 8
        })

        const coreStroke = new GeoJsonLayer({
            id: 'hover-highlight-core',
            ...shared,
            getLineColor: [225, 65, 65],
            getLineWidth: 2,
            lineWidthMinPixels: 2,
            lineWidthMaxPixels: 6
        });
        return [outlineStroke, coreStroke]
    }

    refreshLayers() {
        this.ensureStaticLayers();
        const highlightLayers = this.buildHighlightLayers();
        const layers = [
            ...this.layerState.base,
            ...(this.layerState.main || []),
            ...this.layerState.border,
            ...(highlightLayers ? [highlightLayers] : [])
        ];
        this.deckgl.setProps({ layers });
    }

    renderDeck(mainLayers) {
        this.layerState.main = mainLayers || [];
        this.refreshLayers();
    }

    setHoveredFeature(feature) {
        const nextId = feature ? this.getFeatureIdFromFeature(feature) : null;
        if (nextId === this.hoverState.featureId) return;
        this.hoverState.featureId = nextId;
        this.hoverState.feature = feature || null;
        if (this.deckgl) this.refreshLayers();
    }
    updateLegend(stats, interpolator, normalizeValue, settings, binner, palette) {

        const legend = document.getElementById('legendContent');

        if (stats.type === 'categorical') {
            legend.replaceChildren();
            this.renderCategoricalLegend(legend, stats, interpolator, settings);
            this.refreshLegendChrome();
            return;
        }

        if (binner && palette) {
            legend.replaceChildren();
            this.renderBinnedLegend(legend, stats, settings, binner, palette);
            this.refreshLegendChrome();
            return;
        }

        const cache = this.ensureGradientLegendCache();
        if (!cache) {
            legend.textContent = "template";
            return;
        }

        legend.replaceChildren(cache.shell);

        const { title, description } = resolveLegendHeader(this.dataState, 'Metric');
        if (cache.titleEl) cache.titleEl.textContent = title || 'Metric';
        if (cache.descEl) {
            cache.descEl.textContent = description;
            cache.descEl.style.display = description ? '' : 'none';
        }

        const cssW = 220, cssH = 12;
        const gradPack = this.sizeCanvas(cache.gradCanvas, cssW, cssH);
        const gctx = gradPack.ctx;
        const dpr = gradPack.dpr;
        const widthPx = Math.max(1, Math.round(cssW * dpr));
        for (let i = 0; i < widthPx; i++) {
            const t = widthPx === 1 ? 0 : i / (widthPx - 1);
            gctx.fillStyle = interpolator(t);
            gctx.fillRect(i / dpr, 0, 1 / dpr, cssH);
        }

        const axisCssW = cssW;
        const axisCssH = Math.max(1, Math.round(cache.axisEl.clientHeight || 26));

        const scale = normalizeValue.scale;
        const stepCount = Math.max(2, Number(settings?.legendSteps || 6));

        const domain = (typeof scale.domain === 'function') ? scale.domain() : [stats.min, stats.max];
        const {
            min: dmin,
            max: dmax,
            hasBelow: hasBelowDomain,
            hasAbove: hasAboveDomain
        } = getDomainFlags(stats, domain);

        const { ticks, ticksT } = computeLinearTicks({
            min: dmin,
            max: dmax,
            steps: stepCount,
            toT: v => scale(v)
        });

        const axis = cache.axis;

        const labels = {
            min: {
                t: 0,
                text: formatEdgeLabel({
                    value: dmin,
                    edge: 'min',
                    hasBelow: hasBelowDomain,
                    hasAbove: hasAboveDomain,
                    formatter: v => this.formatValue(v, settings, stats)
                })
            },
            max: {
                t: 1,
                text: formatEdgeLabel({
                    value: dmax,
                    edge: 'max',
                    hasBelow: hasBelowDomain,
                    hasAbove: hasAboveDomain,
                    formatter: v => this.formatValue(v, settings, stats)
                })
            }
        };

        const midLabel = getMidTickLabel({
            ticks,
            steps: stepCount,
            toT: v => scale(v),
            formatter: v => this.formatValue(v, settings, stats)
        });
        if (midLabel) labels.mid = midLabel;

        if (axis) {
            renderSvgAxis({
                axis,
                width: axisCssW,
                height: axisCssH,
                ticksT,
                labels,
                orientation: 'x',
                baseOffset: 2,
                tickLen: 6,
                tickDir: 1,
                labelPad: 2,
                axisLineClass: 'legend-axis-line',
                tickClass: 'legend-axis-tick'
            });
        }


        cache.badgeEl.textContent = formatScaleLabel({
            settings,
            scaleObj: normalizeValue?.scale,
            fallback: 'linear'
        });

        this.refreshLegendChrome();
    }

    renderBinnedLegend(container, stats, settings, binner, palette) {
        const head = buildLegendHeader(this.dataState, 'Metric');
        const scaleLabel = formatScaleLabel({ settings, fallback: 'linear' });
        const badgeText = `${binner.method} (${binner.bins}) | ${scaleLabel}`;

        const paletteSafe = this.expandPalette(palette || [], binner.bins);
        const edges = Array.isArray(binner.edges) ? binner.edges : null;
        const showEdges = edges && edges.length >= binner.bins + 1;

        const rows = [];
        for (let i = 0; i < binner.bins; i++) {
            const labelText = showEdges
                ? formatBinnedRangeLabel({
                    index: i,
                    count: binner.bins,
                    left: this.formatValue(edges[i], settings, stats),
                    right: this.formatValue(edges[i + 1], settings, stats)
                })
                : `Bin ${i + 1}`;
            rows.push({
                color: paletteSafe[i] || '#cccccc',
                label: labelText
            });
        }

        const list = buildLegendList(rows, badgeText);
        container.appendChild(this.wrapLegendForToggle(head, list));
    }

    renderCategoricalLegend(container, stats, interpolator, settings) {
        const head = buildLegendHeader(this.dataState, 'Categories');

        const categories = stats.categories || [];
        if (!categories.length) {
            const empty = document.createElement('div');
            empty.textContent = 'No categorical values';
            container.appendChild(empty);
            return;
        }

        const colorOf = (value) => {
            try {
                if (interpolator?.scale) return interpolator.scale(value);
                return interpolator ? interpolator(value) : '#cccccc';
            } catch {
                return '#cccccc';
            }
        };

        const rows = categories.map(cat => {
            const count = Number.isFinite(cat.count) ? cat.count : null;
            const countText = count != null ? ` (${count.toLocaleString()})` : '';
            return {
                color: colorOf(cat.value) || '#cccccc',
                label: `${this.formatCategoryValue(cat.value)}${countText}`
            };
        });

        const list = buildLegendList(rows);
        container.appendChild(this.wrapLegendForToggle(head, list));
    }

    updateBivariateLegend(bivar, xStats, yStats, xScale, yScale, interpX, interpY, xSet, ySet, blendMode = 'additive', xBinner = null, yBinner = null, paletteInfo = null) {

        this.legendState.cleanup?.();
        this.legendState.cleanup = null;

        const legend = document.getElementById('legendContent');
        legend.replaceChildren();

        const tpl = document.getElementById('bivar-legend-template');
        if (!tpl) {
            legend.textContent = "legend template missing (#bivar-legend-template)";
            return;
        }

        const frag = tpl.content.cloneNode(true);

        const headEl = frag.querySelector('.bivar-legend-text');
        const bodyEl = frag.querySelector('.bivar-legend-container');

        const titleEl = frag.querySelector('.bivar-title');
        const descEl = frag.querySelector('.bivar-description');
        const canvasWrap = frag.querySelector('.bivar-canvas-wrap');

        const yLabelEl = frag.querySelector('.bivar-y-label');
        const xLabelEl = frag.querySelector('.bivar-x-label');
        const xAxisEl = frag.querySelector('.bivar-x-axis');
        const yAxisEl = frag.querySelector('.bivar-y-axis');

        const infoX = frag.querySelector('.bivar-info-x');

        if (titleEl) titleEl.textContent = bivar?.label || 'Bivariate';
        const desc = bivar?.description || '';
        if (descEl) {
            descEl.textContent = desc;
            descEl.style.display = desc ? '' : 'none';
        }

        const yLabelText = bivar?.y?.metric || '';
        const xLabelText = bivar?.x?.metric || '';
        if (yLabelEl) yLabelEl.textContent = '';
        if (xLabelEl) xLabelEl.textContent = '';

        const blend = (c1, c2) => {
            if (blendMode === 'multiply') {
                return [
                    Math.round((c1.r * c2.r) / 255),
                    Math.round((c1.g * c2.g) / 255),
                    Math.round((c1.b * c2.b) / 255)
                ];
            } else if (blendMode === 'screen') {
                return [
                    255 - Math.round(((255 - c1.r) * (255 - c2.r)) / 255),
                    255 - Math.round(((255 - c1.g) * (255 - c2.g)) / 255),
                    255 - Math.round(((255 - c1.b) * (255 - c2.b)) / 255)
                ];
            }
            return [
                Math.max(0, Math.min(255, c1.r + c2.r - 255)),
                Math.max(0, Math.min(255, c1.g + c2.g - 255)),
                Math.max(0, Math.min(255, c1.b + c2.b - 255))
            ];
        };

        const xlinkNS = 'http://www.w3.org/1999/xlink';
        let svgEl = canvasWrap.querySelector('.bivar-svg');
        if (!svgEl) {
            svgEl = document.createElementNS(SVG_NS, 'svg');
            svgEl.classList.add('bivar-svg');
            svgEl.setAttribute('aria-hidden', 'true');
            svgEl.setAttribute('focusable', 'false');
            canvasWrap.replaceChildren(svgEl);
        }

        let imageEl = svgEl.querySelector('.bivar-svg-image');
        if (!imageEl) {
            imageEl = document.createElementNS(SVG_NS, 'image');
            imageEl.classList.add('bivar-svg-image');
            imageEl.setAttribute('preserveAspectRatio', 'none');
            svgEl.appendChild(imageEl);
        }

        let rotGroup = svgEl.querySelector('.bivar-svg-rot');
        if (!rotGroup) {
            rotGroup = document.createElementNS(SVG_NS, 'g');
            rotGroup.classList.add('bivar-svg-rot');
            svgEl.appendChild(rotGroup);
        }
        if (imageEl.parentNode !== rotGroup) rotGroup.appendChild(imageEl);

        let axisGroup = rotGroup.querySelector('.bivar-svg-axes');
        if (!axisGroup) {
            axisGroup = document.createElementNS(SVG_NS, 'g');
            axisGroup.classList.add('bivar-svg-axes');
            rotGroup.appendChild(axisGroup);
        }

        let labelGroup = rotGroup.querySelector('.bivar-svg-labels');
        if (!labelGroup) {
            labelGroup = document.createElementNS(SVG_NS, 'g');
            labelGroup.classList.add('bivar-svg-labels');
            rotGroup.appendChild(labelGroup);
        }

        const bivarCanvas = document.createElement('canvas');

        const stepsX = Math.max(2, Number(xSet?.legendSteps || 4));
        const stepsY = Math.max(2, Number(ySet?.legendSteps || 4));
        const resolvedPalette = paletteInfo || this.resolveBivarPalette(bivar, xSet, ySet);
        const hasPalette = !!resolvedPalette;
        const isBinned = hasPalette || xBinner || yBinner;
        const binsX = hasPalette ? resolvedPalette.binsX : (xBinner?.bins || stepsX);
        const binsY = hasPalette ? resolvedPalette.binsY : (yBinner?.bins || stepsY);
        const paletteGrid = hasPalette ? resolvedPalette.grid.map(row => row.map(color => d3.rgb(color))) : null;

        const xDom = (typeof xScale.scale.domain === 'function') ? xScale.scale.domain() : [xStats.min, xStats.max];
        const yDom = (typeof yScale.scale.domain === 'function') ? yScale.scale.domain() : [yStats.min, yStats.max];
        const {
            min: xMin,
            max: xMax,
            hasBelow: xHasBelowDomain,
            hasAbove: xHasAboveDomain
        } = getDomainFlags(xStats, xDom);
        const {
            min: yMin,
            max: yMax,
            hasBelow: yHasBelowDomain,
            hasAbove: yHasAboveDomain
        } = getDomainFlags(yStats, yDom);

        const invertScale = (scale, t, min, max) => {
            if (scale && typeof scale.invert === 'function') return Number(scale.invert(t));
            return min + t * (max - min);
        };

        let ticksX = [];
        let ticksY = [];
        let edgeTX = [];
        let edgeTY = [];

        if (isBinned) {
            edgeTX = d3.range(binsX + 1).map(i => i / binsX);
            edgeTY = d3.range(binsY + 1).map(i => i / binsY);
            ticksX = edgeTX.slice();
            ticksY = edgeTY.slice();
        } else {
            if (typeof xScale.scale.invert === 'function') ticksX = d3.range(stepsX).map(i => Number(xScale.scale.invert(i / (stepsX - 1))));
            else ticksX = d3.range(stepsX).map(i => xMin + (i / (stepsX - 1)) * (xMax - xMin));
            ticksX[0] = xMin;
            ticksX[ticksX.length - 1] = xMax;
            ticksX = ticksX.map(Number).filter(Number.isFinite);

            if (typeof yScale.scale.invert === 'function') ticksY = d3.range(stepsY).map(i => Number(yScale.scale.invert(i / (stepsY - 1))));
            else ticksY = d3.range(stepsY).map(i => yMin + (i / (stepsY - 1)) * (yMax - yMin));
            ticksY[0] = yMin;
            ticksY[ticksY.length - 1] = yMax;
            ticksY = ticksY.map(Number).filter(Number.isFinite);
        }

        if (xAxisEl) xAxisEl.replaceChildren();
        if (yAxisEl) yAxisEl.replaceChildren();

        const formatEdge = (value, idx, maxIdx, hasBelow, hasAbove, set, stats) => {
            const edge = idx == null ? null : (idx === 0 ? 'min' : (idx === maxIdx ? 'max' : null));
            return formatEdgeLabel({
                value,
                edge,
                hasBelow,
                hasAbove,
                formatter: v => this.formatValue(v, set, stats)
            });
        };


        const xLabelTicks = [];
        const yLabelTicks = [];

        if (isBinned) {
            const xEdgeValues = (xBinner?.edges && xBinner.edges.length === binsX + 1)
                ? xBinner.edges
                : edgeTX.map(t => invertScale(xScale.scale, t, xMin, xMax));
            xEdgeValues.forEach((value, i) => {
                const t = edgeTX[i];
                const label = formatEdge(value, i, xEdgeValues.length - 1, xHasBelowDomain, xHasAboveDomain, xSet, xStats);
                xLabelTicks.push({ t, label });
            });
        } else {
            const minLabel = formatEdge(xMin, 0, 1, xHasBelowDomain, xHasAboveDomain, xSet, xStats);
            const maxLabel = formatEdge(xMax, 1, 1, xHasBelowDomain, xHasAboveDomain, xSet, xStats);
            xLabelTicks.push({ t: 0, label: minLabel });
            xLabelTicks.push({ t: 1, label: maxLabel });
        }

        if (isBinned) {
            const yEdgeValues = (yBinner?.edges && yBinner.edges.length === binsY + 1)
                ? yBinner.edges
                : edgeTY.map(t => invertScale(yScale.scale, t, yMin, yMax));
            yEdgeValues.forEach((value, i) => {
                const t = edgeTY[i];
                const label = formatEdge(value, i, yEdgeValues.length - 1, yHasBelowDomain, yHasAboveDomain, ySet, yStats);
                yLabelTicks.push({ t, label });
            });
        } else {
            const minLabel = formatEdge(yMin, 0, 1, yHasBelowDomain, yHasAboveDomain, ySet, yStats);
            const maxLabel = formatEdge(yMax, 1, 1, yHasBelowDomain, yHasAboveDomain, ySet, yStats);
            yLabelTicks.push({ t: 0, label: minLabel });
            yLabelTicks.push({ t: 1, label: maxLabel });
        }

        const ticksTX = (isBinned ? ticksX : ticksX.map(v => xScale.scale(v)))
            .map(Number)
            .filter(Number.isFinite);
        const ticksTY = (isBinned ? ticksY : ticksY.map(v => yScale.scale(v)))
            .map(Number)
            .filter(Number.isFinite);

        const xBadge = formatScaleLabel({
            settings: xSet,
            scaleObj: xScale?.scale,
            fallback: 'linear'
        });

        infoX.textContent = isBinned ? `${xBadge} | ${binsX}x${binsY} bins` : xBadge;
        legend.replaceChildren(this.wrapLegendForToggle(headEl, bodyEl));

        const readCssPx = (el) => {
            const r = el.getBoundingClientRect();
            return {
                w: Math.max(1, Math.round(r.width)),
                h: Math.max(1, Math.round(r.height))
            };
        };

        const setSvgAttrs = (el, attrs) => {
            Object.entries(attrs).forEach(([key, value]) => {
                if (value == null) return;
                el.setAttribute(key, String(value));
            });
        };

        const makeSvgEl = (tag, attrs, className) => {
            const el = document.createElementNS(SVG_NS, tag);
            if (className) el.setAttribute('class', className);
            if (attrs) setSvgAttrs(el, attrs);
            return el;
        };

        const getRotateDeg = () => {
            const raw = getComputedStyle(canvasWrap).getPropertyValue('--bivar-rotate').trim();
            if (!raw) return -45;
            if (raw.endsWith('deg')) {
                const parsed = parseFloat(raw.replace('deg', '').trim());
                return Number.isFinite(parsed) ? parsed : -45;
            }
            const parsed = parseFloat(raw);
            return Number.isFinite(parsed) ? parsed : -45;
        };

        const render = () => {
            const { w: wrapW, h: wrapH } = readCssPx(canvasWrap);
            const diamondSize = Math.max(1, Math.min(wrapW, wrapH));
            if (!Number.isFinite(diamondSize) || diamondSize <= 0) return;

            const baseSize = diamondSize / Math.SQRT2;
            const inset = (diamondSize - baseSize) / 2;

            svgEl.setAttribute('viewBox', `0 0 ${diamondSize} ${diamondSize}`);
            svgEl.setAttribute('width', `${diamondSize}`);
            svgEl.setAttribute('height', `${diamondSize}`);
            imageEl.setAttribute('width', `${baseSize}`);
            imageEl.setAttribute('height', `${baseSize}`);
            imageEl.setAttribute('x', `${inset}`);
            imageEl.setAttribute('y', `${inset}`);

            const pack = this.sizeCanvas(bivarCanvas, baseSize, baseSize, {
                style: { imageRendering: 'pixelated' }
            });

            if (isBinned) {
                pack.ctx.clearRect(0, 0, pack.cssW, pack.cssH);
                const cellW = pack.cssW / binsX;
                const cellH = pack.cssH / binsY;
                for (let row = 0; row < binsY; row++) {
                    for (let col = 0; col < binsX; col++) {
                        let color = null;
                        if (hasPalette) {
                            color = paletteGrid?.[row]?.[col];
                            if (!color) continue;
                            pack.ctx.fillStyle = color.formatRgb();
                        } else {
                            const tx = (col + 0.5) / binsX;
                            const ty = 1 - (row + 0.5) / binsY;
                            const c1 = d3.rgb(interpX(tx));
                            const c2 = d3.rgb(interpY(ty));
                            const [r, g, b] = blend(c1, c2);
                            pack.ctx.fillStyle = `rgb(${r}, ${g}, ${b})`;
                        }
                        const x0 = col * cellW;
                        const y0 = row * cellH;
                        const x1 = (col + 1) * cellW;
                        const y1 = (row + 1) * cellH;
                        pack.ctx.fillRect(x0, y0, x1 - x0, y1 - y0);
                    }
                }
            } else {
                const imgData = pack.ctx.createImageData(pack.pxW, pack.pxH);
                const xColors = new Array(pack.pxW);
                const yColors = new Array(pack.pxH);

                for (let x = 0; x < pack.pxW; x++) {
                    const tx = pack.pxW === 1 ? 0 : x / (pack.pxW - 1);
                    xColors[x] = d3.rgb(interpX(tx));
                }
                for (let y = 0; y < pack.pxH; y++) {
                    const ty = 1 - (pack.pxH === 1 ? 0 : y / (pack.pxH - 1));
                    yColors[y] = d3.rgb(interpY(ty));
                }

                for (let y = 0; y < pack.pxH; y++) {
                    const c2 = yColors[y];
                    for (let x = 0; x < pack.pxW; x++) {
                        const c1 = xColors[x];
                        const [r, g, b] = blend(c1, c2);
                        const idx = (y * pack.pxW + x) * 4;
                        imgData.data[idx] = r;
                        imgData.data[idx + 1] = g;
                        imgData.data[idx + 2] = b;
                        imgData.data[idx + 3] = 255;
                    }
                }

                pack.ctx.setTransform(1, 0, 0, 1, 0, 0);
                pack.ctx.putImageData(imgData, 0, 0);
                pack.ctx.setTransform(pack.dpr, 0, 0, pack.dpr, 0, 0);
            }

            const dataUrl = bivarCanvas.toDataURL();
            imageEl.setAttribute('href', dataUrl);
            imageEl.setAttributeNS(xlinkNS, 'href', dataUrl);

            const center = diamondSize / 2;
            const rotateDeg = getRotateDeg();
            rotGroup.setAttribute('transform', `rotate(${rotateDeg} ${center} ${center})`);

            axisGroup.replaceChildren();
            labelGroup.replaceChildren();

            const tickLen = 6;
            const labelPad = 6;
            const titlePad = Math.max(10, baseSize * 0.08);

            axisGroup.appendChild(makeSvgEl('line', {
                x1: inset,
                y1: inset + baseSize,
                x2: inset + baseSize,
                y2: inset + baseSize
            }, 'bivar-svg-axis'));

            axisGroup.appendChild(makeSvgEl('line', {
                x1: inset,
                y1: inset,
                x2: inset,
                y2: inset + baseSize
            }, 'bivar-svg-axis'));

            ticksTX.forEach((t) => {
                if (!(t >= 0 && t <= 1)) return;
                const x = inset + t * baseSize;
                axisGroup.appendChild(makeSvgEl('line', {
                    x1: x,
                    y1: inset + baseSize,
                    x2: x,
                    y2: inset + baseSize + tickLen
                }, 'bivar-svg-tick'));
            });

            ticksTY.forEach((t) => {
                if (!(t >= 0 && t <= 1)) return;
                const y = inset + (1 - t) * baseSize;
                axisGroup.appendChild(makeSvgEl('line', {
                    x1: inset,
                    y1: y,
                    x2: inset - tickLen,
                    y2: y
                }, 'bivar-svg-tick'));
            });

            xLabelTicks.forEach(({ t, label }) => {
                if (!label) return;
                const x = inset + t * baseSize;
                const text = makeSvgEl('text', {
                    x,
                    y: inset + baseSize + tickLen + labelPad
                }, 'bivar-svg-tick-label bivar-svg-tick-label-x');
                text.textContent = label;
                labelGroup.appendChild(text);
            });

            yLabelTicks.forEach(({ t, label }) => {
                if (!label) return;
                const y = inset + (1 - t) * baseSize;
                const text = makeSvgEl('text', {
                    x: inset - tickLen - labelPad,
                    y
                }, 'bivar-svg-tick-label bivar-svg-tick-label-y');
                text.setAttribute('transform', `rotate(90 ${inset - tickLen - labelPad} ${y})`);
                text.textContent = label;
                labelGroup.appendChild(text);
            });

            if (xLabelText) {
                const text = makeSvgEl('text', {
                    x: inset + baseSize / 2,
                    y: inset + baseSize + tickLen + labelPad + titlePad
                }, 'bivar-svg-axis-label bivar-svg-axis-label-x');
                text.textContent = xLabelText;
                labelGroup.appendChild(text);
            }

            if (yLabelText) {
                const text = makeSvgEl('text', {
                    x: inset - tickLen - labelPad - titlePad,
                    y: inset + baseSize / 2
                }, 'bivar-svg-axis-label bivar-svg-axis-label-y');
                text.setAttribute('transform', `rotate(90 ${inset - tickLen - labelPad - titlePad} ${inset + baseSize / 2})`);
                text.textContent = yLabelText;
                labelGroup.appendChild(text);
            }
        };

        const ro = new ResizeObserver(render);
        ro.observe(canvasWrap);
        window.addEventListener('resize', render, { passive: true });

        this.legendState.cleanup = () => {
            ro.disconnect();
            window.removeEventListener('resize', render);
        };

        render();
        this.refreshLegendChrome();
    }

    onHover(info) {
        if (this.inputState.mode === 'touch') {
            return;
        }
        if (!info.object || !this.dataState.currentData) {
            this.hideTooltip();
            this.setHoveredFeature(null);
            return;
        }

        const feature = info.object;
        const { clone, contentKey } = this.buildTooltipPayload(feature);
        if (this.tooltipState.suppressKey === contentKey && performance.now() < this.tooltipState.suppressUntil) {
            return;
        }

        this.setHoveredFeature(feature);
        if (this.tooltipState.visible || this.tooltipState.showDelay <= 0) {
            this.showTooltipNow(info, clone, contentKey);
            return;
        }

        if (this.tooltipState.changeTimer) {
            clearTimeout(this.tooltipState.changeTimer);
            this.tooltipState.changeTimer = 0;
        }
        this.tooltipState.pendingChange = null;
        this.tooltipState.pending = { info, clone, contentKey };
        if (!this.tooltipState.showTimer) {
            this.tooltipState.showTimer = setTimeout(() => {
                this.tooltipState.showTimer = 0;
                const pending = this.tooltipState.pending;
                this.tooltipState.pending = null;
                if (!pending) return;
                this.showTooltipNow(pending.info, pending.clone, pending.contentKey);
            }, this.tooltipState.showDelay);
        }

    }

    onClick(info) {
        if (this.inputState.mode !== 'touch' && !this.isTouchInput(info?.srcEvent)) {
            return;
        }
        if (!info?.object || !this.dataState.currentData) {
            this.hideTooltip();
            this.setHoveredFeature(null);
            return;
        }

        const feature = info.object;
        const { clone, contentKey } = this.buildTooltipPayload(feature);
        const isSameFeature = this.tooltipState.visible && this.tooltipState.contentKey === contentKey;

        if (isSameFeature) {
            this.tooltipState.suppressKey = contentKey;
            this.tooltipState.suppressUntil = performance.now() + 350;
            this.hideTooltip();
            this.setHoveredFeature(null);
            return;
        }

        if (this.tooltipState.showTimer) {
            clearTimeout(this.tooltipState.showTimer);
            this.tooltipState.showTimer = 0;
        }
        if (this.tooltipState.changeTimer) {
            clearTimeout(this.tooltipState.changeTimer);
            this.tooltipState.changeTimer = 0;
        }
        this.tooltipState.pending = null;
        this.tooltipState.pendingChange = null;
        this.tooltipState.pendingKey = '';

        this.setHoveredFeature(feature);
        this.showTooltipNow(info, clone, contentKey);
    }

    // is there some more clever way to do this?

    setupInputModeListeners(container) {
        if (!container) return;
        const setMode = (mode) => {
            if (this.inputState.mode === mode) return;
            this.inputState.mode = mode;
            this.inputState.lastSwitch = performance.now();
        };
        const onPointerDown = (event) => {
            if (event?.pointerType === 'touch' || event?.pointerType === 'pen') {
                setMode('touch');
                return;
            }
            if (event?.pointerType === 'mouse') {
                setMode('mouse');
            }
        };
        const onTouchStart = () => setMode('touch');
        const onMouseDown = () => setMode('mouse');

        container.addEventListener('pointerdown', onPointerDown, { passive: true });
        container.addEventListener('touchstart', onTouchStart, { passive: true });
        container.addEventListener('mousedown', onMouseDown, { passive: true });

        this.inputState.cleanup = () => {
            container.removeEventListener('pointerdown', onPointerDown);
            container.removeEventListener('touchstart', onTouchStart);
            container.removeEventListener('mousedown', onMouseDown);
        };
    }

    isTouchInput(srcEvent = null) {
        if (typeof window === 'undefined') return false;
        if (srcEvent) {
            const type = String(srcEvent.type || '');
            if (type.startsWith('touch')) return true;
            if (srcEvent.pointerType === 'touch') return true;
        }
        const supportsMatch = typeof window.matchMedia === 'function';
        const coarse = supportsMatch && (
            window.matchMedia('(pointer: coarse)').matches
            || window.matchMedia('(any-pointer: coarse)').matches
        );
        const noHover = supportsMatch && window.matchMedia('(hover: none)').matches;
        const touchPoints = navigator.maxTouchPoints > 0 || navigator.msMaxTouchPoints > 0;
        const touchEvent = 'ontouchstart' in window;
        return Boolean(coarse || noHover || touchPoints || touchEvent);
    }

    hideTooltip() {
        if (!this.tooltipState.el) return;
        this.tooltipState.el.style.display = 'none';
        this.tooltipState.el.style.visibility = 'hidden';
        if (this.tooltipState.raf) {
            cancelAnimationFrame(this.tooltipState.raf);
            this.tooltipState.raf = 0;
        }
        this.tooltipState.nextPos = null;
        if (this.tooltipState.showTimer) {
            clearTimeout(this.tooltipState.showTimer);
            this.tooltipState.showTimer = 0;
        }
        this.tooltipState.pending = null;
        if (this.tooltipState.changeTimer) {
            clearTimeout(this.tooltipState.changeTimer);
            this.tooltipState.changeTimer = 0;
        }
        this.tooltipState.pendingChange = null;
        this.tooltipState.pendingKey = '';
        this.tooltipState.visible = false;
        this.tooltipState.contentKey = '';
        this.tooltipState.el.style.opacity = '';
    }

    buildTooltipPayload(feature) {
        const tpl = document.getElementById('tooltip-template');
        const clone = tpl.content.cloneNode(true);
        const props = feature.properties || {};
        const featureId = this.getFeatureIdFromFeature(feature);
        const nameFromFile = app.dataManager.getFeatureNameByFeature(this.dataState.currentData, feature);
        const nameFromProps = props.NAME || props.name || '';
        const fallbackTitle = nameFromProps || featureId || feature.id;
        const title = nameFromFile || fallbackTitle || '';
        clone.querySelector('.tooltip-title').textContent = title;
        const subtitleEl = clone.querySelector('.tooltip-subtitle');
        if (subtitleEl) {
            const subtitle = featureId && featureId !== title ? featureId : '';
            subtitleEl.textContent = subtitle;
            subtitleEl.style.display = subtitle ? '' : 'none';
        }

        if (this.bivarState.bivar) {
            clone.querySelector('.tooltip-metric').textContent = this.bivarState.bivar.label || 'Bivariate';
            const xData = this.bivarState.data?.x || this.dataState.currentData;
            const yData = this.bivarState.data?.y || this.dataState.currentData;
            const vx = app.dataManager.getFeatureValueByFeature(xData, feature, this.bivarState.bivar.x.unit);
            const vy = app.dataManager.getFeatureValueByFeature(yData, feature, this.bivarState.bivar.y.unit);
            clone.querySelector('.tooltip-value').textContent = `${this.formatValue(vx, this.bivarState.bivar.x.settings)} | ${this.formatValue(vy, this.bivarState.bivar.y.settings)}`;
        }
        else if (this.dataState.currentMetric) {
            const v = app.dataManager.getFeatureValueByFeature(this.dataState.currentData, feature, this.dataState.currentMetric);
            clone.querySelector('.tooltip-metric').textContent = this.dataState.currentMetricLabel || this.dataState.currentMetricId || 'Metric';
            clone.querySelector('.tooltip-value').textContent = this.formatValue(v, this.dataState.currentMetricSettings);
        }

        const contentKey = this.getTooltipContentKey(featureId);
        return { clone, contentKey };
    }

    showTooltipNow(info, clone, contentKey) {
        this.tooltipState.el.style.display = 'block';
        this.tooltipState.el.style.visibility = 'visible';
        this.tooltipState.visible = true;

        if (this.tooltipState.changeDelay > 0 && this.tooltipState.contentKey && contentKey !== this.tooltipState.contentKey) {
            if (this.tooltipState.pendingKey !== contentKey) {
                if (this.tooltipState.changeTimer) {
                    clearTimeout(this.tooltipState.changeTimer);
                    this.tooltipState.changeTimer = 0;
                }
                this.tooltipState.pendingKey = contentKey;
            }
            this.tooltipState.el.style.opacity = '0.15';
            this.tooltipState.pendingChange = { info, clone, contentKey };
            if (!this.tooltipState.changeTimer) {
                this.tooltipState.changeTimer = setTimeout(() => {
                    this.tooltipState.changeTimer = 0;
                    const pending = this.tooltipState.pendingChange;
                    this.tooltipState.pendingChange = null;
                    if (!pending) return;
                    this.tooltipState.pendingKey = '';
                    this.applyTooltipContent(pending.clone, pending.contentKey);
                    this.updateTooltipPosition(pending.info);
                }, this.tooltipState.changeDelay);
            }
        } else {
            if (this.tooltipState.changeTimer) {
                clearTimeout(this.tooltipState.changeTimer);
                this.tooltipState.changeTimer = 0;
            }
            this.tooltipState.pendingChange = null;
            this.tooltipState.pendingKey = '';
            this.applyTooltipContent(clone, contentKey);
        }
        this.updateTooltipPosition(info);
    }

    applyTooltipContent(clone, contentKey) {
        if (contentKey !== this.tooltipState.contentKey) {
            this.tooltipState.contentKey = contentKey;
            this.tooltipState.el.replaceChildren(clone);
            this.tooltipState.size = {
                w: this.tooltipState.el.offsetWidth,
                h: this.tooltipState.el.offsetHeight
            };
        }
        this.tooltipState.el.style.opacity = '1';
        this.tooltipState.el.style.visibility = 'visible';
    }

    getTooltipContentKey(featureId) {
        const metricKey = this.bivarState.bivar
            ? (this.bivarState.bivar.key || 'bivar')
            : (this.dataState.currentMetricId || this.dataState.currentMetric || '');
        return `${featureId}::${metricKey}`;
    }

    updateTooltipPosition(info) {
        const x0 = Number(info?.x);
        const y0 = Number(info?.y);
        if (!Number.isFinite(x0) || !Number.isFinite(y0)) return;
        this.scheduleTooltipPosition(x0, y0);
    }

    scheduleTooltipPosition(x0, y0) {
        this.tooltipState.nextPos = { x: x0, y: y0 };
        if (this.tooltipState.raf) return;
        this.tooltipState.raf = requestAnimationFrame(() => {
            this.tooltipState.raf = 0;
            const next = this.tooltipState.nextPos;
            this.tooltipState.nextPos = null;
            if (!next) return;
            this.applyTooltipPosition(next.x, next.y);
        });
    }

    applyTooltipPosition(x0, y0) {
        const offset = 12;
        const padding = 8;
        const container = this.tooltipState.el.offsetParent || this.tooltipState.el.parentElement || document.body;
        const rect = container.getBoundingClientRect();
        let tooltipW = this.tooltipState.size.w;
        let tooltipH = this.tooltipState.size.h;
        if (!(tooltipW > 0 && tooltipH > 0)) {
            tooltipW = this.tooltipState.el.offsetWidth;
            tooltipH = this.tooltipState.el.offsetHeight;
            this.tooltipState.size = { w: tooltipW, h: tooltipH };
        }
        let x = x0 + offset;
        let y = y0 + offset;
        if (x + tooltipW + padding > rect.width && x0 - offset - tooltipW >= padding) {
            x = x0 - offset - tooltipW;
        }
        if (y + tooltipH + padding > rect.height && y0 - offset - tooltipH >= padding) {
            y = y0 - offset - tooltipH;
        }
        const maxX = Math.max(padding, rect.width - tooltipW - padding);
        const maxY = Math.max(padding, rect.height - tooltipH - padding);
        x = Math.min(Math.max(padding, x), maxX);
        y = Math.min(Math.max(padding, y), maxY);
        this.tooltipState.el.style.transform = `translate3d(${Math.round(x)}px, ${Math.round(y)}px, 0)`;
    }

    computeViewFromBounds(bounds, container) {
        const [minX, minY, maxX, maxY] = bounds;
        const width = Math.max(1, container?.clientWidth || 800);
        const height = Math.max(1, container?.clientHeight || 600);
        const spanX = Math.max(1, (maxX - minX) * 1.05);
        const spanY = Math.max(1, (maxY - minY) * 1.05);
        const scale = Math.min(width / spanX, height / spanY);
        const zoom = Math.log2(Math.max(1e-6, scale));
        return {
            target: [(minX + maxX) / 2, (minY + maxY) / 2, 0],
            zoom,
            pitch: 0, bearing: 0
        };
    }

    formatValue(v, settings = null, stats = null) {
        if (v == null) return 'N/A';
        if (v === Infinity) return 'Γê₧';
        if (v === -Infinity) return '-Γê₧';
        if (!Number.isFinite(v)) return 'N/A';

        const format = settings?.format || null;
        const hasExplicitFormat = !!format && typeof format === 'object';
        const useIntl = hasExplicitFormat || settings?.percentage === true;

        const cacheIntl = (options) => {
            const key = JSON.stringify(options);
            const hit = this.cacheState.intl.get(key);
            if (hit) return hit;
            const nf = new Intl.NumberFormat(undefined, options);
            this.cacheState.intl.set(key, nf);
            return nf;
        };

        const resolvePercentScale = () => {
            const explicit = String(settings?.percentageScale || '').toLowerCase();
            if (explicit === 'fraction' || explicit === 'percent') return explicit;
            const domain = Array.isArray(settings?.domain) && settings.domain.length ? settings.domain : null;
            const max = Number(domain?.[domain?.length - 1] ?? stats?.max);
            if (Number.isFinite(max) && max > 1.5) return 'percent';
            return 'fraction';
        };

        if (settings?.percentage === true) {
            const scale = resolvePercentScale();
            const normalized = scale === 'percent' ? (v / 100) : v;
            const maximumFractionDigits = Number.isInteger(format?.maximumFractionDigits) ? format.maximumFractionDigits : 2;
            const minimumFractionDigits = Number.isInteger(format?.minimumFractionDigits) ? format.minimumFractionDigits : 0;
            const options = {
                style: 'percent',
                notation: format?.notation || 'standard',
                maximumFractionDigits,
                minimumFractionDigits
            };
            return cacheIntl(options).format(normalized);
        }

        if (useIntl) {
            if (typeof format?.specifier === 'string' && format.specifier.trim()) {
                try {
                    return d3.format(format.specifier)(v);
                } catch {
                    return String(v);
                }
            }
            const options = {
                style: 'decimal',
                notation: format?.notation || 'standard',
                useGrouping: format?.useGrouping !== false
            };
            if (Number.isInteger(format?.maximumFractionDigits)) options.maximumFractionDigits = format.maximumFractionDigits;
            if (Number.isInteger(format?.minimumFractionDigits)) options.minimumFractionDigits = format.minimumFractionDigits;
            if (Number.isInteger(format?.maximumSignificantDigits)) options.maximumSignificantDigits = format.maximumSignificantDigits;
            if (Number.isInteger(format?.minimumSignificantDigits)) options.minimumSignificantDigits = format.minimumSignificantDigits;
            return cacheIntl(options).format(v);
        }

        if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(1) + 'M';
        if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1) + 'k';
        if (Math.abs(v) < 0.1 && v !== 0) return v.toExponential(1);
        return v.toLocaleString(undefined, { maximumFractionDigits: 1 });
    }

    getDisplayTransform(settings = {}, stats) {
        if (settings?.percentage !== true) return { toDisplay: v => v, fromDisplay: v => v };
        const domain = Array.isArray(settings?.domain) && settings.domain.length ? settings.domain : null;
        const max = Number(domain?.[domain?.length - 1] ?? stats?.max);
        const explicit = String(settings?.percentageScale || '').toLowerCase();
        const scale = (explicit === 'percent' || explicit === 'fraction')
            ? explicit
            : (Number.isFinite(max) && max > 1.5 ? 'percent' : 'fraction');
        if (scale === 'percent') return { toDisplay: v => v, fromDisplay: v => v };
        return { toDisplay: v => Number(v) * 100, fromDisplay: v => Number(v) / 100 };
    }

    formatCategoryValue(value) {
        if (value == null || value === '') return 'Not specified';
        return String(value);
    }
}

class ChoroplethApp {
    constructor() {
        this.dataManager = new DataManager();
        this.boundaryManager = new BoundaryManager();
        this.mapRenderer = new MapRenderer('map');
        this.state = this.#initState();
        this.selections = this.#initSelections();
        this.controls = this.#initControls();
    }

    #initState() {
        return {
            currentCategory: null,
            currentMetric: null,
            currentUnitId: null,
            currentData: null,
            activeComposite: null
        };
    }

    #initSelections() {
        return {
            categorical: new Map(),
            composite: new Map()
        };
    }

    #initControls() {
        return {
            filter: []
        };
    }

    async initialize() {
        try {
            document.getElementById('loading').style.display = 'block';
            await this.dataManager.initialize();
            await this.boundaryManager.loadBoundaries();
            this.populateCategorySelect();
            this.mapRenderer.initialize({ bounds: this.boundaryManager.bounds });
            this.setupEventListeners();
            const categories = Object.keys(this.dataManager.indexes.get('category') || {});
            const initialCategory = categories[0];
            await this.loadCategory(initialCategory);
            document.getElementById('loading').style.display = 'none';
        } catch (error) {
            console.error('failed init', error);
        }
    }

    setupEventListeners() {
        document.getElementById('categorySelect').addEventListener('change', e => {
            void this.loadCategory(e.target.value);
        });
    }

    populateCategorySelect() {
        const select = document.getElementById('categorySelect');
        const index = this.dataManager.indexes.get('category') || {};
        select.innerHTML = '';
        Object.keys(index).forEach(k => {
            const opt = document.createElement('option');
            opt.value = k;
            opt.textContent = index[k]?.label || k.toUpperCase();
            select.appendChild(opt);
        });
    }

    async loadCategory(category) {
        this.state.currentCategory = category;
        this.state.currentData = await this.dataManager.loadCategoryData(category);
        this.updateMetricSelector();
        const cfg = this.dataManager.indexes.get('category')[category];
        const firstMetric = (cfg.metricsOrder && cfg.metricsOrder[0]) || Object.keys(cfg.metrics || {})[0];
        if (firstMetric) await this.selectMetric(firstMetric);
    }

    updateMetricSelector() {
        const container = document.getElementById('metricSelector');
        container.innerHTML = '';
        const cfg = this.dataManager.indexes.get('category')[this.state.currentCategory];

        const metricIds = (Array.isArray(cfg.metricsOrder) && cfg.metricsOrder.length)
            ? cfg.metricsOrder
            : Object.keys(cfg.metrics || {});
        metricIds.forEach(metricId => {
            const metric = cfg.metrics?.[metricId];
            if (!metric) return;
            const isBivar = metric.kind === 'bivar' || (metric.x && metric.y);
            const btn = document.createElement('div');
            btn.className = 'metric-button';
            btn.dataset.key = `metric:${metricId}`;
            btn.textContent = isBivar ? `(x/y) ${metric.label || metricId}` : (metric.label || metricId);
            btn.title = metric.description || '';
            btn.onclick = () => { void this.selectMetric(metricId); };
            container.appendChild(btn);
        });
    }

    async selectMetric(metric) {

        const cfg = this.getCurrentCategoryConfig();
        const def = cfg?.metrics?.[metric];
        if (!def) {
            return;
        }

        if (def.kind === 'bivar' || (def.x && def.y)) {
            await this.selectBivariateMetric(metric, def, cfg);
            return;
        }

        this.state.currentMetric = metric;
        this.setActiveButton(`metric:${metric}`);

        const { unit, composite } = await this.resolveMetricSource(cfg, metric);
        if (!unit) return;
        this.state.currentUnitId = unit;
        this.state.activeComposite = composite;

        const stats = this.state.currentData ? this.dataManager.getPropertyStats(this.state.currentData, unit) : null;
        let catFilter = null;
        if (stats?.type === 'categorical') {
            const selection = this.ensureCategoricalSelection(unit, stats.categories);
            catFilter = new Set(selection);
        }

        this.mapRenderer.updateLayers(
            this.boundaryManager.features,
            this.state.currentData,
            unit,
            metric,
            cfg,
            null,
            catFilter
        );

        this.setupFilterUI(false, cfg, unit, metric, null, { stats });
    }

    async selectBivariateMetric(metricId, def, cfg) {

        this.setActiveButton(`metric:${metricId}`);
        this.state.currentMetric = null;
        this.state.currentUnitId = null;
        this.state.activeComposite = null;

        const normalizeAxis = (axis, fallbackLabel) => {
            if (typeof axis === 'string') return { metricId: axis, label: fallbackLabel || null };
            if (axis && typeof axis === 'object') {
                const metricKey = axis.metricId || axis.id || axis.metric || axis.key;
                return {
                    metricId: metricKey,
                    label: axis.label || axis.valueLabel || fallbackLabel || null,
                    settings: axis.settings || null,
                    category: axis.category || axis.sourceCategory || axis.cat || null
                };
            }
            return { metricId: null, label: fallbackLabel || null };
        };

        const getMetric = (id, categoryCfg) => categoryCfg?.metrics?.[id] || null;
        const resolveField = async (id, category, categoryCfg, dataRef) => {
            const m = getMetric(id, categoryCfg);
            if (!m) return null;
            if (typeof m.field === 'string') {
                await this.dataManager.ensureMetricsLoaded(category, [m.field], dataRef);
                return m.field;
            }
            if (m.composite && dataRef) {
                const selection = this.getCompositeSelectionSet(id, m.composite, category);
                const parts = (m.composite.parts || []).filter(p => selection.has(p));
                await this.dataManager.ensureMetricsLoaded(category, parts, dataRef);
                const composite = this.dataManager.getCompositeBuffer(category, id, m.composite, parts, dataRef);
                return composite?.key || null;
            }
            return null;
        };

        const xAxis = normalizeAxis(def.x, def.xLabel);
        const yAxis = normalizeAxis(def.y, def.yLabel);

        const xCategory = xAxis.category || this.state.currentCategory;
        const yCategory = yAxis.category || this.state.currentCategory;
        const xCfg = this.dataManager.getCategoryConfig(xCategory) || {};
        const yCfg = this.dataManager.getCategoryConfig(yCategory) || {};

        const [xData, yData] = await Promise.all([
            this.dataManager.loadCategoryData(xCategory),
            this.dataManager.loadCategoryData(yCategory)
        ]);

        const xMetric = getMetric(xAxis.metricId, xCfg);
        const yMetric = getMetric(yAxis.metricId, yCfg);
        const xUnit = await resolveField(xAxis.metricId, xCategory, xCfg, xData);
        const yUnit = await resolveField(yAxis.metricId, yCategory, yCfg, yData);
        if (!xMetric || !yMetric || !xUnit || !yUnit) {
            return;
        }

        const xSettings = { ...(xMetric.settings || {}), ...(xAxis.settings || {}) };
        const ySettings = { ...(yMetric.settings || {}), ...(yAxis.settings || {}) };

        const bivar = {
            key: metricId,
            label: def.label || metricId,
            description: def.description || null,
            x: {
                metricId: xAxis.metricId,
                metric: xAxis.label || xMetric.label || xAxis.metricId,
                unit: xUnit,
                settings: xSettings,
                category: xCategory
            },
            y: {
                metricId: yAxis.metricId,
                metric: yAxis.label || yMetric.label || yAxis.metricId,
                unit: yUnit,
                settings: ySettings,
                category: yCategory
            },
            method: def.method || {},
            blendMode: def.method?.blendMode || 'additive'
        };

        this.mapRenderer.updateBivariateLayers(
            this.boundaryManager.features,
            { x: xData, y: yData },
            bivar,
            { x: xCfg, y: yCfg },
            null
        );

        this.setupFilterUI(true, cfg, null, null, bivar);
    }

    setActiveButton(key) {
        document.querySelectorAll('.metric-button').forEach(b => {
            if (b.dataset.key === key) b.classList.add('active');
            else b.classList.remove('active');
        });
    }

    setupFilterUI(isBivar, cfg, unit, metricKey, bivarDef, options = {}) {
        const container = document.getElementById('filterControls');
        const compositeContainer = document.getElementById('compositeControls');
        container.replaceChildren();
        this.controls.filter = [];
        if (compositeContainer) {
            compositeContainer.replaceChildren();
            compositeContainer.classList.add('is-hidden');
        }

        const addRangeControl = ({ label, description, onChange, formatValue }) => {
            const el = document.createElement('range-filter-control');
            container.appendChild(el);
            el.setConfig({ label, description, onChange, formatValue });
            this.controls.filter.push(el);
            return el;
        };

        if (isBivar) {
            const createHandler = (axis) => (min, max) => {
                const current = this.mapRenderer.filterState.bivar;
                current[axis] = { min, max };
                this.mapRenderer.updateBivariateLayers(
                    this.boundaryManager.features,
                    this.mapRenderer.bivarState.data,
                    this.mapRenderer.bivarState.bivar,
                    this.mapRenderer.bivarState.cfg,
                    current
                );
            };

            const xData = this.mapRenderer.bivarState.data?.x || this.state.currentData;
            const yData = this.mapRenderer.bivarState.data?.y || this.state.currentData;
            const xCfg = this.mapRenderer.bivarState.cfg?.x || cfg;
            const yCfg = this.mapRenderer.bivarState.cfg?.y || cfg;

            const statsX = this.dataManager.getPropertyStats(xData, bivarDef.x.unit);
            const setX = bivarDef.x.settings || (xCfg.metrics && xCfg.metrics[bivarDef.x.metricId]?.settings) || {};
            const normX = this.mapRenderer.buildScaler(statsX, setX);
            const xTx = this.mapRenderer.getDisplayTransform(setX, statsX);

            addRangeControl({
                label: `X: ${bivarDef.x.metric}`,
                description: xCfg.metrics?.[bivarDef.x.metricId]?.description || '',
                onChange: createHandler('x'),
                formatValue: (v) => this.mapRenderer.formatValue(v, setX, statsX),
                toDisplay: xTx.toDisplay,
                fromDisplay: xTx.fromDisplay
            }).update(statsX, setX, null, normX);

            const statsY = this.dataManager.getPropertyStats(yData, bivarDef.y.unit);
            const setY = bivarDef.y.settings || (yCfg.metrics && yCfg.metrics[bivarDef.y.metricId]?.settings) || {};
            const normY = this.mapRenderer.buildScaler(statsY, setY);
            const yTx = this.mapRenderer.getDisplayTransform(setY, statsY);

            addRangeControl({
                label: `Y: ${bivarDef.y.metric}`,
                description: yCfg.metrics?.[bivarDef.y.metricId]?.description || '',
                onChange: createHandler('y'),
                formatValue: (v) => this.mapRenderer.formatValue(v, setY, statsY),
                toDisplay: yTx.toDisplay,
                fromDisplay: yTx.fromDisplay
            }).update(statsY, setY, null, normY);

        } else {
            const stats = options.stats || this.dataManager.getPropertyStats(this.state.currentData, unit);
            if (!stats) {
                container.textContent = 'Metric statistics unavailable';
                return;
            }

            const metricDef = cfg?.metrics?.[metricKey] || {};
            const settings = metricDef.settings || {};
            const label = metricDef.label || metricKey;
            const description = metricDef.description || '';

            if (stats.type === 'numeric') {
                const norm = this.mapRenderer.buildScaler(stats, settings);
                const currentRange = options.currentRange || this.clampFilterRange(stats, this.mapRenderer.filterState.range);
                const tx = this.mapRenderer.getDisplayTransform(settings, stats);

                addRangeControl({
                    label,
                    description,
                    onChange: (min, max) => {
                        this.applyCurrentMetric({ filterRange: { min, max } });
                    },
                    formatValue: (v) => this.mapRenderer.formatValue(v, settings, stats),
                    toDisplay: tx.toDisplay,
                    fromDisplay: tx.fromDisplay
                }).update(stats, settings, currentRange, norm);

                if (this.state.activeComposite && this.state.activeComposite.metric === metricKey && compositeContainer) {
                    this.renderCompositeControls(compositeContainer);
                    compositeContainer.classList.remove('is-hidden');
                }
            } else if (stats.type === 'categorical') {
                this.renderCategoricalFilter(container, stats, settings, label, unit, description);
            } else {
                container.textContent = 'Unsupported metric type';
            }
        }
    }

    getCurrentCategoryConfig() {
        const index = this.dataManager.indexes.get('category') || {};
        return index[this.state.currentCategory] || {};
    }

    async resolveMetricSource(cfg, metric) {
        const metricDef = cfg?.metrics?.[metric] || null;
        if (!metricDef) return { unit: metric, composite: null };

        if (typeof metricDef.field === 'string') {
            await this.dataManager.ensureMetricsLoaded(this.state.currentCategory, [metricDef.field], this.state.currentData);
            return { unit: metricDef.field, composite: null };
        }

        const definition = metricDef.composite || null;
        if (!definition || !this.state.currentData) return { unit: metric, composite: null };

        const selection = this.getCompositeSelectionSet(metric, definition);
        const parts = (definition.parts || []).filter(part => selection.has(part));
        await this.dataManager.ensureMetricsLoaded(this.state.currentCategory, parts, this.state.currentData);
        const composite = this.dataManager.getCompositeBuffer(this.state.currentCategory, metric, definition, parts, this.state.currentData);
        if (!composite) return { unit: metric, composite: null };

        return { unit: composite.key, composite: { metric, definition, selection } };
    }

    getCompositeSelectionSet(metric, definition, categoryOverride = null) {
        const categoryKey = categoryOverride || this.state.currentCategory;
        const key = `${categoryKey}::${metric}`;
        let selection = this.selections.composite.get(key);
        if (!selection) {
            const defaults = (definition.default && definition.default.length) ? definition.default : definition.parts;
            selection = new Set(defaults || []);
            this.selections.composite.set(key, selection);
            return selection;
        }
        const allowed = new Set(definition.parts || []);
        [...selection].forEach(value => {
            if (!allowed.has(value)) selection.delete(value);
        });
        if (!selection.size) {
            (definition.default && definition.default.length ? definition.default : definition.parts || [])
                .forEach(value => selection.add(value));
        }
        return selection;
    }

    applyCurrentMetric({ filterRange = this.mapRenderer.filterState.range, catFilter = this.mapRenderer.filterState.categorical } = {}) {
        if (!this.state.currentMetric || !this.state.currentUnitId) return;
        const cfg = this.getCurrentCategoryConfig();
        this.mapRenderer.updateLayers(
            this.boundaryManager.features,
            this.state.currentData,
            this.state.currentUnitId,
            this.state.currentMetric,
            cfg,
            filterRange,
            catFilter
        );
    }

    clampFilterRange(stats, range) {
        if (!stats || stats.type !== 'numeric' || !range) return null;
        const min = Number.isFinite(range.min) ? Math.max(stats.min, Math.min(stats.max, range.min)) : stats.min;
        const max = Number.isFinite(range.max) ? Math.max(stats.min, Math.min(stats.max, range.max)) : stats.max;
        return { min, max };
    }

    ensureCategoricalSelection(unit, categories = []) {
        const key = `${this.state.currentCategory}::${unit}`;
        const values = categories.map(cat => cat.value);
        let selection = this.selections.categorical.get(key);
        if (!selection) {
            selection = new Set(values);
            this.selections.categorical.set(key, selection);
            return selection;
        }
        const available = new Set(values);
        [...selection].forEach(value => {
            if (!available.has(value)) selection.delete(value);
        });
        if (!selection.size) values.forEach(value => selection.add(value));
        return selection;
    }

    renderCategoricalFilter(container, stats, settings, metricLabel, unit, description = '') {
        const categories = stats.categories || [];
        if (!categories.length) {
            const msg = document.createElement('div');
            msg.textContent = 'No categorical values to filter';
            container.appendChild(msg);
            return;
        }

        const selection = this.ensureCategoricalSelection(unit, categories);
        const colorizer = this.mapRenderer.buildInterpolator(settings, stats);

        const panel = document.createElement('div');
        panel.className = 'categorical-filter-panel';
        panel.style.display = 'flex';
        panel.style.flexDirection = 'column';
        panel.style.gap = '6px';

        const header = document.createElement('div');
        header.style.display = 'flex';
        header.style.justifyContent = 'space-between';
        header.style.alignItems = 'center';

        const title = document.createElement('label');
        title.textContent = `${metricLabel} filters`;
        title.title = description || '';
        header.appendChild(title);

        const actions = document.createElement('div');
        actions.style.display = 'flex';
        actions.style.gap = '6px';
        const makeBtn = (label, handler) => {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.textContent = label;
            btn.className = 'btn-small';
            btn.onclick = handler;
            return btn;
        };
        const refreshers = [];
        const sync = () => refreshers.forEach(fn => fn());
        const apply = () => this.applyCurrentMetric({ catFilter: new Set(selection) });

        actions.appendChild(makeBtn('All', () => {
            selection.clear();
            categories.forEach(cat => selection.add(cat.value));
            sync();
            apply();
        }));
        actions.appendChild(makeBtn('None', () => {
            selection.clear();
            sync();
            apply();
        }));
        header.appendChild(actions);
        panel.appendChild(header);

        const list = document.createElement('div');
        list.style.display = 'flex';
        list.style.flexDirection = 'column';
        list.style.gap = '4px';

        categories.forEach(cat => {
            const row = document.createElement('label');
            row.style.display = 'flex';
            row.style.alignItems = 'center';
            row.style.gap = '8px';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = selection.has(cat.value);
            checkbox.onchange = () => {
                if (checkbox.checked) selection.add(cat.value);
                else selection.delete(cat.value);
                apply();
            };

            const swatch = document.createElement('span');
            swatch.style.width = '1em';
            swatch.style.height = '1em';
            swatch.style.border = '1px solid rgba(0,0,0,0.25)';
            try {
                swatch.style.background = colorizer?.scale ? colorizer.scale(cat.value) : colorizer(cat.value);
            } catch {
                swatch.style.background = '#cccccc';
            }

            const label = document.createElement('span');
            const count = Number.isFinite(cat.count) ? ` (${cat.count.toLocaleString()})` : '';
            label.textContent = `${this.mapRenderer.formatCategoryValue(cat.value)}${count}`;

            row.appendChild(checkbox);
            row.appendChild(swatch);
            row.appendChild(label);
            list.appendChild(row);

            refreshers.push(() => { checkbox.checked = selection.has(cat.value); });
        });

        panel.appendChild(list);
        container.appendChild(panel);
    }

    getCompositePartMeta(definition, part) {
        const meta = definition?.partLabels?.[part];
        if (!meta) return { label: part, description: '' };
        if (typeof meta === 'string') return { label: meta, description: '' };
        if (typeof meta === 'object') {
            return {
                label: meta.label || part,
                description: meta.description || ''
            };
        }
        return { label: part, description: '' };
    }

    renderCompositeControls(container) {
        const info = this.state.activeComposite;
        if (!info || !info.definition?.parts?.length || !container) return;

        const tpl = document.getElementById('composite-controls-template');
        if (!tpl) return;

        const fragment = tpl.content.cloneNode(true);
        const panel = fragment.querySelector('.composite-controls');
        const title = fragment.querySelector('.composite-controls-title');
        const actions = fragment.querySelector('.composite-controls-actions');
        const grid = fragment.querySelector('.composite-controls-grid');
        const btnAll = actions?.querySelector('[data-action="all"]');
        const btnNone = actions?.querySelector('[data-action="none"]');
        const btnDefault = actions?.querySelector('[data-action="default"]');

        if (title) title.textContent = info.definition.label || 'Composite components';
        if (btnAll) btnAll.onclick = () => { void this.updateCompositeSelection(info.definition.parts); };
        if (btnNone) btnNone.onclick = () => { void this.updateCompositeSelection([]); };
        if (btnDefault) {
            if (info.definition.default?.length) {
                btnDefault.onclick = () => { void this.updateCompositeSelection(info.definition.default); };
            } else {
                btnDefault.remove();
            }
        }

        info.definition.parts.forEach(part => {
            const row = document.createElement('label');
            row.className = 'composite-controls-item';
            const { label, description } = this.getCompositePartMeta(info.definition, part);

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = info.selection.has(part);
            checkbox.onchange = () => {
                if (checkbox.checked) info.selection.add(part);
                else info.selection.delete(part);
                void this.updateCompositeSelection();
            };

            const text = document.createElement('span');
            text.textContent = label;
            if (description) text.title = description;

            row.appendChild(checkbox);
            row.appendChild(text);
            grid?.appendChild(row);
        });

        if (panel) {
            container.replaceChildren(fragment);
        } else {
            container.replaceChildren(fragment);
        }
    }

    async updateCompositeSelection(partsOverride) {
        if (!this.state.activeComposite) return;
        const { definition, selection, metric } = this.state.activeComposite;
        if (Array.isArray(partsOverride)) {
            selection.clear();
            partsOverride.forEach(part => selection.add(part));
        }

        const cfg = this.getCurrentCategoryConfig();
        const parts = (definition.parts || []).filter(part => selection.has(part));
        await this.dataManager.ensureMetricsLoaded(this.state.currentCategory, parts, this.state.currentData);
        const composite = this.dataManager.getCompositeBuffer(this.state.currentCategory, metric, definition, parts, this.state.currentData);
        if (!composite) return;

        this.state.currentUnitId = composite.key;
        const stats = this.dataManager.getPropertyStats(this.state.currentData, composite.key);
        const range = this.clampFilterRange(stats, this.mapRenderer.filterState.range);

        this.mapRenderer.updateLayers(
            this.boundaryManager.features,
            this.state.currentData,
            composite.key,
            metric,
            cfg,
            range,
            null
        );

        this.setupFilterUI(false, cfg, composite.key, metric, null, { stats, currentRange: range });
    }
}

class RangeFilterControlElement extends HTMLElement {
    constructor() {
        super();
        Object.assign(this, this.#initState());
    }

    #initState() {
        return {
            _configured: false,
            onChange: () => {},
            formatValue: v => v,
            toDisplay: v => v,
            fromDisplay: v => v,
            stats: null,
            settings: null,
            scaleObj: null,
            normalizeValue: null,
            domain: { min: 0, max: 1 },
            hasBelowDomain: false,
            hasAboveDomain: false,
            currentRange: null,
            dom: null,
            _handlers: null,
            _axis: null
        };
    }

    connectedCallback() {
        if (this._configured) return;

        const tpl = document.getElementById('range-filter-template');
        if (!tpl) throw new Error('#range-filter-template template missing');

        this.replaceChildren(tpl.content.cloneNode(true));

        const root = this;
        const host = root.querySelector('.range-filter-block');
        this.dom = {
            host,
            label: root.querySelector('.range-filter-label'),
            range: root.querySelector('.dual-range'),
            minPct: root.querySelector('.thumb-min'),
            maxPct: root.querySelector('.thumb-max'),
            minInput: root.querySelector('.input-min'),
            maxInput: root.querySelector('.input-max'),
            valDisplay: root.querySelector('.filter-value-display'),
            axis: root.querySelector('.metric-axis')
        };

        this.#attachEvents();
        this._configured = true;
    }

    setConfig(options = {}) {
        this.onChange = options.onChange || (() => {});
        this.formatValue = options.formatValue || (v => v);
        this.toDisplay = options.toDisplay || (v => v);
        this.fromDisplay = options.fromDisplay || (v => v);

        const label = options.label || '';
        if (this.dom?.label) {
            this.dom.label.textContent = label;
            this.dom.label.title = options.description || '';
            this.dom.label.style.display = label ? '' : 'none';
        }
    }

    update(stats, settings, currentRange, normalizeValue) {
        this.stats = stats;
        this.settings = settings;
        this.scaleObj = normalizeValue?.scale;
        this.normalizeValue = normalizeValue;

        const rawDomain = (this.scaleObj && typeof this.scaleObj.domain === 'function')
            ? this.scaleObj.domain()
            : [stats.min, stats.max];

        const { min: dmin, max: dmax, hasBelow, hasAbove } = getDomainFlags(stats, rawDomain);
        this.domain = { min: dmin, max: dmax };
        this.hasBelowDomain = hasBelow;
        this.hasAboveDomain = hasAbove;

        this.currentRange = currentRange || { min: dmin, max: dmax };

        const tMin = this.#tOf(this.currentRange.min);
        const tMax = this.#tOf(this.currentRange.max);

        this.dom.minPct.value = Math.round(tMin * 100);
        this.dom.maxPct.value = Math.round(tMax * 100);

        this.dom.minInput.value = this.#roundForInput(this.toDisplay(this.currentRange.min));
        this.dom.maxInput.value = this.#roundForInput(this.toDisplay(this.currentRange.max));

        this.#renderAxis(dmin, dmax);
        this.#updateVisuals(tMin, tMax);
    }

    #attachEvents() {
        const { minPct, maxPct, minInput, maxInput } = this.dom;

        const commitRange = (ta, tb) => {
            const lo = Math.min(ta, tb);
            const hi = Math.max(ta, tb);

            const vMinInDomain = this.#vOf(lo);
            const vMaxInDomain = this.#vOf(hi);

            const minBound = (lo <= 0 && this.hasBelowDomain) ? -Infinity : vMinInDomain;
            const maxBound = (hi >= 1 && this.hasAboveDomain) ? Infinity : vMaxInDomain;

            this.onChange(minBound, maxBound);
        };

        const onSliderInput = () => {
            const ta = Math.min(minPct.value, maxPct.value) / 100;
            const tb = Math.max(minPct.value, maxPct.value) / 100;
            this.#updateVisuals(ta, tb);

            const vMin = this.#vOf(ta);
            const vMax = this.#vOf(tb);
            minInput.value = this.#roundForInput(this.toDisplay(vMin));
            maxInput.value = this.#roundForInput(this.toDisplay(vMax));
        };

        const onSliderCommit = () => {
            const ta = Math.min(minPct.value, maxPct.value) / 100;
            const tb = Math.max(minPct.value, maxPct.value) / 100;
            commitRange(ta, tb);
        };

        minPct.addEventListener('input', onSliderInput);
        maxPct.addEventListener('input', onSliderInput);
        minPct.addEventListener('change', onSliderCommit);
        maxPct.addEventListener('change', onSliderCommit);

        const onTextCommit = () => {
            let vMin = this.fromDisplay(this.#parseNumber(minInput.value));
            let vMax = this.fromDisplay(this.#parseNumber(maxInput.value));

            if (!Number.isFinite(vMin)) vMin = this.domain.min;
            if (!Number.isFinite(vMax)) vMax = this.domain.max;
            if (vMin > vMax) [vMin, vMax] = [vMax, vMin];

            const clampedMin = Math.max(this.domain.min, Math.min(this.domain.max, vMin));
            const clampedMax = Math.max(this.domain.min, Math.min(this.domain.max, vMax));

            const ta = this.#tOf(clampedMin);
            const tb = this.#tOf(clampedMax);

            this.dom.minPct.value = Math.round(ta * 100);
            this.dom.maxPct.value = Math.round(tb * 100);
            this.#updateVisuals(ta, tb);

            const lo = Math.min(ta, tb);
            const hi = Math.max(ta, tb);

            const minBound = ((vMin <= this.domain.min || lo <= 0) && this.hasBelowDomain) ? -Infinity : clampedMin;
            const maxBound = ((vMax >= this.domain.max || hi >= 1) && this.hasAboveDomain) ? Infinity : clampedMax;

            this.onChange(minBound, maxBound);
        };
        minInput.addEventListener('change', onTextCommit);
        maxInput.addEventListener('change', onTextCommit);
        minInput.addEventListener('keydown', e => e.key === 'Enter' && onTextCommit());
        maxInput.addEventListener('keydown', e => e.key === 'Enter' && onTextCommit());
    }

    #updateVisuals(ta, tb) {
        const lo = Math.min(ta, tb);
        const hi = Math.max(ta, tb);

        this.dom.range.style.setProperty('--ta', lo);
        this.dom.range.style.setProperty('--tb', hi);

        const vMin = this.#vOf(lo);
        const vMax = this.#vOf(hi);

        if (this.dom.valDisplay) {
            const leftValue = (lo <= 0) ? this.domain.min : vMin;
            const rightValue = (hi >= 1) ? this.domain.max : vMax;

            const left = formatEdgeLabel({
                value: leftValue,
                edge: lo <= 0 ? 'min' : null,
                hasBelow: this.hasBelowDomain,
                hasAbove: this.hasAboveDomain,
                formatter: v => this.formatValue(v)
            });

            const right = formatEdgeLabel({
                value: rightValue,
                edge: hi >= 1 ? 'max' : null,
                hasBelow: this.hasBelowDomain,
                hasAbove: this.hasAboveDomain,
                formatter: v => this.formatValue(v)
            });

            this.dom.valDisplay.textContent = `${left} to ${right}`;
        }
    }

    #renderAxis(dmin, dmax) {

        const axis = this.dom.axis;
        if (!axis) {
            return;
        }

        const cssW = Math.max(1, Math.round(axis.clientWidth || 220));
        const cssH = Math.max(1, Math.round(axis.clientHeight || 14));

        axis.style.position = axis.style.position || 'relative';

        if (!this._axis) {
            this._axis = buildAxisSvgFromTemplate(axis, {
                svgClass: 'metric-axis-svg',
                labelClass: 'metric-axis-label'
            });
        }

        const axisPack = this._axis;
        if (!axisPack) return;

        const majorCount = Math.max(2, Number(this.settings?.legendSteps || 6));
        const { ticksT } = computeLinearTicks({
            min: dmin,
            max: dmax,
            steps: majorCount,
            toT: v => this.#tOf(v)
        });

        const labels = {
            min: {
                t: 0,
                text: formatEdgeLabel({
                    value: dmin,
                    edge: 'min',
                    hasBelow: this.hasBelowDomain,
                    hasAbove: this.hasAboveDomain,
                    formatter: v => this.formatValue(v)
                })
            },
            max: {
                t: 1,
                text: formatEdgeLabel({
                    value: dmax,
                    edge: 'max',
                    hasBelow: this.hasBelowDomain,
                    hasAbove: this.hasAboveDomain,
                    formatter: v => this.formatValue(v)
                })
            }
        };

        renderSvgAxis({
            axis: axisPack,
            width: cssW,
            height: cssH,
            ticksT,
            labels,
            orientation: 'x',
            baseOffset: 2,
            tickLen: 6,
            tickDir: 1,
            labelPad: 2,
            drawAxisLine: false,
            axisLineClass: 'metric-axis-line',
            tickClass: 'metric-axis-tick'
        });

    }

    #tOf(v) {
        if (!this.normalizeValue) return 0;
        return Math.max(0, Math.min(1, this.normalizeValue(Number(v))));
    }

    #vOf(t) {
        const tClamped = Math.max(0, Math.min(1, t));
        if (this.scaleObj && typeof this.scaleObj.invert === 'function') {
            const rng = this.scaleObj.range();
            const r0 = rng[0], r1 = rng[rng.length - 1];
            const valInRange = r0 + tClamped * (r1 - r0);
            return this.scaleObj.invert(valInRange);
        }
        return this.domain.min + tClamped * (this.domain.max - this.domain.min);
    }

    #parseNumber(raw) {
        if (raw == null) return NaN;
        let s = String(raw).trim().replace(/,/g, '');
        if (s === '') return NaN;
        if (s.endsWith('%')) s = s.slice(0, -1).trim();
        const m = s.match(/^(-?\d*\.?\d*)([kKmMbBtT])$/i);
        if (m) {
            const val = parseFloat(m[1]);
            const unit = m[2].toLowerCase();
            const mult = { k: 1e3, m: 1e6, b: 1e9, t: 1e12 }[unit] || 1;
            return val * mult;
        }
        return Number(s);
    }

    #roundForInput(x) {
        if (!Number.isFinite(x)) return '';
        const abs = Math.abs(x);
        if (abs === 0) return '0';
        if (abs < 0.001) return x.toExponential(2);
        if (abs > 10000) return Math.round(x);
        return parseFloat(x.toPrecision(4));
    }
}

customElements.define('range-filter-control', RangeFilterControlElement);

const app = new ChoroplethApp();
app.initialize();
