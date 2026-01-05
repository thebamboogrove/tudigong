import * as d3 from 'd3';
import { ckmeans } from 'simple-statistics';
import { Deck, OrthographicView, COORDINATE_SYSTEM } from '@deck.gl/core';
import { GeoJsonLayer, PathLayer, PolygonLayer } from '@deck.gl/layers';
import { PathStyleExtension } from '@deck.gl/extensions';
import { feature as topojsonFeature, mesh as topojsonMesh } from 'topojson-client';

class BoundaryManager {
    constructor() {
        this.boundaries = null;
        this.features = null;
        this.featureIndex = new Map();
        this.bounds = null;
        this.countryFeatures = null;
        this.provinceMeshFeature = null;
        this.extraPaths = null;
        this.tendashPaths = null;
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

        const provMeshGeom = topojsonMesh(topology, topology.objects[provinceKey], (a, b) => a !== b);
        this.provinceMeshFeature = [{ type: 'Feature', geometry: provMeshGeom, properties: { level: 'province-mesh' } }];

        this.countyMeshGeom = topojsonMesh(topology, topology.objects[countyKey], (a, b) => a !== b);
        this.provinceMeshGeom = topojsonMesh(topology, topology.objects[provinceKey], (a, b) => a !== b);

        const cleanKey = k => String(k || '').replace(/[\uFEFF\u200B\u202F\u00A0\u2000-\u200A\u2028\u2029]/g, '').trim();

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
            const normalizeId = v => {
                if (v == null) return '';
                return String(v).replace(/[\uFEFF\u200B\u202F\u00A0\u2000-\u200A\u2028\u2029]/g, '').trim();
            };
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
        this.cache = new Map();
        this.indexes = new Map();
        this.loadingPromises = new Map();
        this.compositeCache = new Map();
        this.packCache = new Map();
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
        if (value == null) return '';
        return String(value)
            .replace(/[\uFEFF\u200B\u202F\u00A0\u2000-\u200A\u2028\u2029]/g, '')
            .trim();
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
        const normalizeId = (v) => {
            if (v == null) return '';
            return String(v)
                .replace(/[\uFEFF\u200B\u202F\u00A0\u2000-\u200A\u2028\u2029]/g, '')
                .trim();
        };
        const id = normalizeId(props.CODE ?? props.code ?? feature.id);
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

class MapRenderer {
    constructor(containerId) {
        this.containerId = containerId;
        this.deckgl = null;
        this.currentData = null;
        this.currentMetric = null;
        this.currentMetricId = null;
        this.currentMetricLabel = null;
        this.currentMetricDescription = null;
        this.currentMetricSettings = null;

        this.bivar = null;
        this.bivarValuesX = null;
        this.bivarValuesY = null;
        this.bivarData = null;
        this.bivarCfg = null;

        this.tooltip = document.getElementById('tooltip');
        this.filterRange = null;
        this.categoricalFilter = new Set();
        this.bivarFilters = { x: null, y: null };
        this._intlCache = new Map();
        this._legendCleanup = null;
        this._legendChromeCleanup = null;
        this._legendCollapsedManual = null;
        this._legendCollapsed = false;
        this._mainLayers = [];
        this._baseLayers = null;
        this._borderLayers = null;
        this._hoveredFeature = null;
        this._hoveredFeatureId = null;
        this._legendCache = null;
    }

    getDpr() {
        return window.devicePixelRatio || 1;
    }

    getCanvasDpr(dprCap = 2) {
        return Math.min(dprCap, this.getDpr());
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
        this._legendCollapsed = !!collapsed;
        legend.classList.toggle('is-collapsed', this._legendCollapsed);
        btn.setAttribute('aria-expanded', String(!this._legendCollapsed));
    }

    syncLegendToggle() {
        const legend = document.getElementById('legend');
        const btn = document.getElementById('legendToggle');
        if (!legend || !btn) return;

        const show = this.hasCollapsibleLegend();
        btn.style.display = show ? 'inline-flex' : 'none';
        if (!show) {
            legend.classList.remove('is-collapsed');
            this._legendCollapsed = false;
            this._legendCollapsedManual = null;
            btn.setAttribute('aria-expanded', 'true');
            return;
        }

        if (this._legendCollapsedManual == null) this.setLegendCollapsed(this.isMobileLegend());
        else this.setLegendCollapsed(this._legendCollapsedManual);
    }

    initLegendChrome() {
        const legend = document.getElementById('legend');
        const btn = document.getElementById('legendToggle');
        if (!legend || !btn) return;

        const applyAuto = () => {
            if (this._legendCollapsedManual != null) return;
            this.setLegendCollapsed(this.isMobileLegend());
        };

        btn.onclick = () => {
            this._legendCollapsedManual = !this._legendCollapsed;
            this.setLegendCollapsed(this._legendCollapsedManual);
        };

        const onResize = this.debounce(applyAuto, 180);
        window.addEventListener('resize', onResize, { passive: true });
        this._legendChromeCleanup = () => window.removeEventListener('resize', onResize);

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
        if (this._legendCache?.type === 'gradient') return this._legendCache;

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
        const axisCanvas = document.createElement('canvas');
        Object.assign(axisCanvas.style, {
            position: 'absolute',
            left: '0',
            top: '0',
            display: 'block',
            width: '220px',
            height: '100%'
        });
        axisEl.replaceChildren(axisCanvas);

        const minLabel = document.createElement('div');
        minLabel.className = 'tick-label';
        axisEl.appendChild(minLabel);

        const maxLabel = document.createElement('div');
        maxLabel.className = 'tick-label';
        axisEl.appendChild(maxLabel);

        const midLabel = document.createElement('div');
        midLabel.className = 'tick-label';
        midLabel.style.display = 'none';
        axisEl.appendChild(midLabel);

        this._legendCache = {
            type: 'gradient',
            shell,
            titleEl,
            descEl,
            canvasWrap,
            axisEl,
            badgeEl,
            gradCanvas,
            axisCanvas,
            axisLabels: { min: minLabel, max: maxLabel, mid: midLabel }
        };
        return this._legendCache;
    }


    draw1DAxisCanvas({ ctx, crisp, cssW, cssH, ticksT, tickLen = 6, baseline = true }) {
        ctx.clearRect(0, 0, cssW, cssH);

        const y0 = crisp(2);

        if (baseline) {
            ctx.strokeStyle = 'rgba(0,0,0,0.35)';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(0, y0);
            ctx.lineTo(cssW, y0);
            ctx.stroke();
        }

        ctx.strokeStyle = 'rgba(0,0,0,0.75)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        for (const t of ticksT) {
            if (!(t >= 0 && t <= 1)) continue;
            const x = crisp(t * (cssW - 1));
            ctx.moveTo(x, y0);
            ctx.lineTo(x, y0 + tickLen);
        }
        ctx.stroke();
    }

    drawYAxisCanvas({ ctx, crisp, cssW, cssH, ticksT, tickLen = 6, baseline = true }) {
        ctx.clearRect(0, 0, cssW, cssH);

        const x0 = crisp(cssW - 2);

        if (baseline) {
            ctx.strokeStyle = 'rgba(0,0,0,0.35)';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(x0, 0);
            ctx.lineTo(x0, cssH);
            ctx.stroke();
        }

        ctx.strokeStyle = 'rgba(0,0,0,0.75)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        for (const t of ticksT) {
            if (!(t >= 0 && t <= 1)) continue;
            const y = crisp((1 - t) * (cssH - 1));
            ctx.moveTo(x0, y);
            ctx.lineTo(x0 - tickLen, y);
        }
        ctx.stroke();
    }

    initialize(options = {}) {
        this.initLegendChrome();
        const container = document.getElementById(this.containerId);
        const initialViewState = options.bounds
            ? this.computeViewFromBounds(options.bounds, container)
            : { target: [0, 0, 0], zoom: 0, pitch: 0, bearing: 0 };
        this.baseZoom = initialViewState.zoom ?? 0;
        console.log('Base zoom: ' + this.baseZoom);

        this.deckgl = new Deck({
            parent: container,
            views: [new OrthographicView({ id: 'ortho', flipY: false })],
            initialViewState,
            controller: true,
            layerFilter: ({layer, viewport}) => {
                if (layer.id === 'borders-county') return viewport.zoom >= (this.baseZoom + 3);
                return true;
            },
            onHover: this.onHover.bind(this),
            onClick: info => console.log('Clicked:', info.object)
        });
    }


    parseNumberLike(v, fallback = NaN) {
        if (typeof v === 'number') return Number.isFinite(v) ? v : fallback;
        if (typeof v === 'string') {
            const s = v.trim();
            if (/^-?\d+(\.\d+)?$/.test(s)) return parseFloat(s);
            const m = s.match(/^\s*(-?\d+(?:\.\d+)?)\s*\/\s*(-?\d+(?:\.\d+)?)\s*$/);
            if (m && parseFloat(m[2]) !== 0) return parseFloat(m[1]) / parseFloat(m[2]);
        }
        return fallback;
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

        this.bivar = bivar;
        this.bivarData = dataByAxis || null;
        this.bivarCfg = cfgByAxis || null;
        if (!this.currentData) {
            this.currentData = dataByAxis?.x || dataByAxis?.y || null;
        }
        this.currentMetricId = null;
        this.currentMetricLabel = bivar?.label || null;
        this.currentMetricDescription = bivar?.description || null;
        this.currentMetricSettings = null;
        this.bivarFilters = filters || { x: null, y: null };
        this.currentMetric = null; // clear

        const xData = dataByAxis?.x || this.currentData;
        const yData = dataByAxis?.y || this.currentData;
        const key = `${bivar.key}::${bivar?.x?.category || ''}::${bivar?.y?.category || ''}`;
        if (!this.bivarValuesX || !this.bivarValuesY || this.bivarKey !== key) {
            this.bivarValuesX = features.map(f => app.dataManager.getFeatureValueByFeature(xData, f, bivar.x.unit));
            this.bivarValuesY = features.map(f => app.dataManager.getFeatureValueByFeature(yData, f, bivar.y.unit));
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
        const xBinner = this.buildBinner(this.bivarValuesX, xStats, xSet, defaultXBins);
        const yBinner = this.buildBinner(this.bivarValuesY, yStats, ySet, defaultYBins);

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
                const vx = this.bivarValuesX[index];
                const vy = this.bivarValuesY[index];

                if (!Number.isFinite(vx) || !Number.isFinite(vy)) return [200,200,200,255];

                if (this.bivarFilters.x && (vx < this.bivarFilters.x.min || vx > this.bivarFilters.x.max)) return [0,0,0,0];
                if (this.bivarFilters.y && (vy < this.bivarFilters.y.min || vy > this.bivarFilters.y.max)) return [0,0,0,0];

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
                    this.bivarFilters?.x?.min, this.bivarFilters?.x?.max,
                    this.bivarFilters?.y?.min, this.bivarFilters?.y?.max,
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

        this.currentData = data;
        this.currentMetric = metric;
        this.currentMetricId = metricKey;
        this.currentMetricLabel = cfg?.metrics?.[metricKey]?.label || metricKey;
        this.currentMetricDescription = cfg?.metrics?.[metricKey]?.description || null;
        this.currentMetricSettings = (cfg?.metrics?.[metricKey]?.settings) || null;
        this.bivar = null; // clear bivariate
        this.filterRange = filterRange;
        this.categoricalFilter = catFilter;

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
                    if (this.categoricalFilter && this.categoricalFilter.size > 0 && !this.categoricalFilter.has(v)) return [0,0,0,0];
                    const c = d3.rgb(interpolator(v));
                    return [c.r, c.g, c.b, 255];
                }

                if (!Number.isFinite(v)) return [200,200,200,255];
                if (this.filterRange && (v < this.filterRange.min || v > this.filterRange.max)) return [0,0,0,0];

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
        if (!this._baseLayers) {
            this._baseLayers = [
                new GeoJsonLayer({
                    id: 'country-fill',
                    data: app.boundaryManager.countryFeatures || [],
                    coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
                    filled: true, stroked: false, pickable: false,
                    getFillColor: [190, 190, 190, 255]
                })
            ];
        }

        if (!this._borderLayers) {
            this._borderLayers = [
                new PathLayer({
                    id: 'borders-county',
                    data: app.boundaryManager.countyPaths || [],
                    coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
                    getPath: d => d, getColor: [55, 55, 55, 255],
                    getWidth: 14,
                    widthUnits: "meters",
                    widthMinPixels: 1,
                    widthMaxPixels: 4,
                    widthScale: 50,
                    jointRounded: true
                }),
                new PathLayer({
                    id: 'borders-province',
                    data: app.boundaryManager.provincePaths || [],
                    coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
                    getPath: d => d, getColor: [30, 30, 30, 255],
                    getWidth: 28,
                    widthUnits: "meters",
                    widthMinPixels: 1,
                    widthMaxPixels: 6,
                    widthScale: 60,
                    jointRounded: true
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
                    getWidth: 28,
                    widthUnits: "meters",
                    widthMinPixels: 1,
                    widthMaxPixels: 6,
                    widthScale: 60,
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

    buildHighlightLayer() {
        if (!this._hoveredFeature) return null;
        return new GeoJsonLayer({
            id: 'hover-highlight',
            data: [this._hoveredFeature],
            coordinateSystem: COORDINATE_SYSTEM.CARTESIAN,
            filled: false,
            stroked: true,
            pickable: false,
            getLineColor: [255, 55, 55, 230],
            getLineWidth: 2,
            lineWidthUnits: 'pixels',
            lineWidthMinPixels: 3,
            lineWidthMaxPixels: 6
        });
    }

    refreshLayers() {
        this.ensureStaticLayers();
        const highlightLayer = this.buildHighlightLayer();
        const layers = [
            ...this._baseLayers,
            ...(this._mainLayers || []),
            ...this._borderLayers,
            ...(highlightLayer ? [highlightLayer] : [])
        ];
        this.deckgl.setProps({ layers });
    }

    renderDeck(mainLayers) {
        this._mainLayers = mainLayers || [];
        this.refreshLayers();
    }

    setHoveredFeature(feature) {
        const nextId = feature ? this.getFeatureIdFromFeature(feature) : null;
        if (nextId === this._hoveredFeatureId) return;
        this._hoveredFeatureId = nextId;
        this._hoveredFeature = feature || null;
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

        if (cache.titleEl) cache.titleEl.textContent = this.currentMetricLabel || this.currentMetricId || 'Metric';
        const desc = this.currentMetricDescription || '';
        if (cache.descEl) {
            cache.descEl.textContent = desc;
            cache.descEl.style.display = desc ? '' : 'none';
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
        const axisCanvasPack = this.sizeCanvas(cache.axisCanvas, axisCssW, axisCssH, {
            style: { position: 'absolute', left: '0', top: '0' }
        });

        const scale = normalizeValue.scale;
        const stepCount = Math.max(2, Number(settings?.legendSteps || 6));

        const domain = (typeof scale.domain === 'function') ? scale.domain() : [stats.min, stats.max];
        const dmin = Math.min(Number(domain[0]), Number(domain[domain.length - 1]));
        const dmax = Math.max(Number(domain[0]), Number(domain[domain.length - 1]));

        let ticks = d3.range(stepCount).map(i => dmin + (i / (stepCount - 1)) * (dmax - dmin));
        ticks[0] = dmin;
        ticks[ticks.length - 1] = dmax;
        ticks = ticks.map(Number).filter(Number.isFinite);

        const ticksT = ticks.map(v => scale(v)).filter(t => t >= 0 && t <= 1);
        this.draw1DAxisCanvas({
            ctx: axisCanvasPack.ctx,
            crisp: axisCanvasPack.crisp,
            cssW: axisCanvasPack.cssW,
            cssH: axisCanvasPack.cssH,
            ticksT,
            tickLen: 6,
            baseline: true
        });

        const eps = (Number.isFinite(stats?.max) && Number.isFinite(stats?.min))
            ? Math.max(1e-12, (stats.max - stats.min) * 1e-9)
            : 1e-12;
        const hasBelowDomain = Number.isFinite(stats?.min) && (stats.min < dmin - eps);
        const hasAboveDomain = Number.isFinite(stats?.max) && (stats.max > dmax + eps);

        if (cache.axisLabels?.min) {
            cache.axisLabels.min.style.left = '0%';
            cache.axisLabels.min.textContent = hasBelowDomain
                ? `≤ ${this.formatValue(dmin, settings, stats)}`
                : this.formatValue(dmin, settings, stats);
        }
        if (cache.axisLabels?.max) {
            cache.axisLabels.max.style.left = '100%';
            cache.axisLabels.max.textContent = hasAboveDomain
                ? `≥ ${this.formatValue(dmax, settings, stats)}`
                : this.formatValue(dmax, settings, stats);
        }

        if (ticks.length <= 3 && ticks.length === stepCount) {
            const midIdx = Math.floor(ticks.length / 2);
            if (midIdx > 0 && midIdx < ticks.length - 1) {
                const tMid = scale(ticks[midIdx]);
                if (cache.axisLabels?.mid) {
                    cache.axisLabels.mid.style.left = `${tMid * 100}%`;
                    cache.axisLabels.mid.textContent = this.formatValue(ticks[midIdx], settings, stats);
                    cache.axisLabels.mid.style.display = '';
                }
            } else if (cache.axisLabels?.mid) {
                cache.axisLabels.mid.style.display = 'none';
            }
        } else if (cache.axisLabels?.mid) {
            cache.axisLabels.mid.style.display = 'none';
        }

        const scaleName = String(settings?.scale || 'linear').toLowerCase();
        if (scaleName === 'pow') {
            const k = normalizeValue.scale.__exponentRaw ?? normalizeValue.scale.__exponent ?? settings.exponent ?? 2;
            cache.badgeEl.textContent = `power scale (k=${k})`;
        } else {
            cache.badgeEl.textContent = scaleName.toLowerCase() + ` scale`;
        }

        this.refreshLegendChrome();
    }

    renderBinnedLegend(container, stats, settings, binner, palette) {
        const head = document.createElement('div');
        const title = document.createElement('div');
        title.className = 'legend-title';
        title.textContent = this.currentMetricLabel || this.currentMetricId || 'Metric';
        head.appendChild(title);

        const desc = this.currentMetricDescription || '';
        if (desc) {
            const description = document.createElement('div');
            description.className = 'legend-description';
            description.textContent = desc;
            head.appendChild(description);
        }

        const badge = document.createElement('div');
        badge.className = 'legend-badge';
        const scaleName = String(settings?.scale || 'linear').toLowerCase();
        badge.textContent = `${binner.method} (${binner.bins}) | ${scaleName} scale`;
        

        const list = document.createElement('div');
        list.className = 'legend-categorical';

        const paletteSafe = this.expandPalette(palette || [], binner.bins);
        const edges = Array.isArray(binner.edges) ? binner.edges : null;
        const showEdges = edges && edges.length >= binner.bins + 1;

        const formatEdgeBin = (i) => {
            const left = this.formatValue(edges[i], settings, stats);
            const right = this.formatValue(edges[i + 1], settings, stats);
            if (i === 0) return `≤ ${right}`;
            if (i === binner.bins - 1) return `≥ ${left}`;
            return `${left} - ${right}`;
        };

        for (let i = 0; i < binner.bins; i++) {
            const row = document.createElement('div');
            row.className = 'legend-row';

            const swatch = document.createElement('span');
            swatch.className = 'legend-swatch';
            swatch.style.background = paletteSafe[i] || '#cccccc';

            const label = document.createElement('span');
            if (showEdges) {
                label.textContent = formatEdgeBin(i);
            } else {
                label.textContent = `Bin ${i + 1}`;
            }

            row.appendChild(swatch);
            row.appendChild(label);
            list.appendChild(row);
        }
        list.appendChild(badge);
        container.appendChild(this.wrapLegendForToggle(head, list));
    }

    renderCategoricalLegend(container, stats, interpolator, settings) {
        const head = document.createElement('div');
        const title = document.createElement('div');
        title.className = 'legend-title';
        title.textContent = this.currentMetricLabel || this.currentMetricId || 'Categories';
        head.appendChild(title);

        const desc = this.currentMetricDescription || '';
        if (desc) {
            const description = document.createElement('div');
            description.className = 'legend-description';
            description.textContent = desc;
            head.appendChild(description);
        }

        const categories = stats.categories || [];
        if (!categories.length) {
            const empty = document.createElement('div');
            empty.textContent = 'No categorical values';
            container.appendChild(empty);
            return;
        }

        const list = document.createElement('div');
        list.className = 'legend-categorical';

        const colorOf = (value) => {
            try {
                if (interpolator?.scale) return interpolator.scale(value);
                return interpolator ? interpolator(value) : '#cccccc';
            } catch {
                return '#cccccc';
            }
        };

        categories.forEach(cat => {
            const row = document.createElement('div');
            row.className = 'legend-row';

            const swatch = document.createElement('span');
            swatch.className = 'legend-swatch';
            swatch.style.background = colorOf(cat.value) || '#cccccc';

            const label = document.createElement('span');
            const count = Number.isFinite(cat.count) ? cat.count : null;
            const countText = count != null ? ` (${count.toLocaleString()})` : '';
            label.textContent = `${this.formatCategoryValue(cat.value)}${countText}`;

            row.appendChild(swatch);
            row.appendChild(label);
            list.appendChild(row);
        });

        container.appendChild(this.wrapLegendForToggle(head, list));
    }

    updateBivariateLegend(bivar, xStats, yStats, xScale, yScale, interpX, interpY, xSet, ySet, blendMode = 'additive', xBinner = null, yBinner = null, paletteInfo = null) {

        this._legendCleanup?.();
        this._legendCleanup = null;

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

        yLabelEl.textContent = bivar.y.metric;
        xLabelEl.textContent = bivar.x.metric;

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

        const bivarCanvas = document.createElement('canvas');
        Object.assign(bivarCanvas.style, {
            display: 'block',
            width: '100%',
            height: '100%',
            imageRendering: 'pixelated'
        });
        canvasWrap.replaceChildren(bivarCanvas);

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
        const xMin = Math.min(Number(xDom[0]), Number(xDom[xDom.length - 1]));
        const xMax = Math.max(Number(xDom[0]), Number(xDom[xDom.length - 1]));
        const yMin = Math.min(Number(yDom[0]), Number(yDom[yDom.length - 1]));
        const yMax = Math.max(Number(yDom[0]), Number(yDom[yDom.length - 1]));

        const xEps = (Number.isFinite(xStats?.max) && Number.isFinite(xStats?.min))
            ? Math.max(1e-12, (xStats.max - xStats.min) * 1e-9)
            : 1e-12;
        const yEps = (Number.isFinite(yStats?.max) && Number.isFinite(yStats?.min))
            ? Math.max(1e-12, (yStats.max - yStats.min) * 1e-9)
            : 1e-12;
        const xHasBelowDomain = Number.isFinite(xStats?.min) && (xStats.min < xMin - xEps);
        const xHasAboveDomain = Number.isFinite(xStats?.max) && (xStats.max > xMax + xEps);
        const yHasBelowDomain = Number.isFinite(yStats?.min) && (yStats.min < yMin - yEps);
        const yHasAboveDomain = Number.isFinite(yStats?.max) && (yStats.max > yMax + yEps);

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

        const makeAxisCanvas = (host) => {
            host.style.position = host.style.position || 'relative';
            host.replaceChildren();
            const canvas = document.createElement('canvas');
            Object.assign(canvas.style, {
                position: 'absolute',
                left: '0',
                top: '0',
                width: '100%',
                height: '100%',
                display: 'block'
            });
            host.appendChild(canvas);
            return canvas;
        };

        const xAxisCanvas = makeAxisCanvas(xAxisEl);
        const yAxisCanvas = makeAxisCanvas(yAxisEl);

        const mkXLabel = (text, t) => {
            const el = document.createElement('div');
            el.className = 'tick-label';
            el.style.left = `${t * 100}%`;
            el.textContent = text;
            return el;
        };

        const formatEdgeLabel = (value, idx, maxIdx, hasBelow, hasAbove, set, stats) => {
            let text = this.formatValue(value, set, stats);
            if (idx === 0 && hasBelow) text = `≤ ${text}`;
            else if (idx === maxIdx && hasAbove) text = `≥ ${text}`;
            return text;
        };

        if (isBinned) {
            const xEdgeValues = (xBinner?.edges && xBinner.edges.length === binsX + 1)
                ? xBinner.edges
                : edgeTX.map(t => invertScale(xScale.scale, t, xMin, xMax));
            xEdgeValues.forEach((value, i) => {
                const t = edgeTX[i];
                const label = formatEdgeLabel(value, i, xEdgeValues.length - 1, xHasBelowDomain, xHasAboveDomain, xSet, xStats);
                xAxisEl.appendChild(mkXLabel(label, t));
            });
        } else {
            const minLabel = formatEdgeLabel(xMin, 0, 1, xHasBelowDomain, xHasAboveDomain, xSet, xStats);
            const maxLabel = formatEdgeLabel(xMax, 1, 1, xHasBelowDomain, xHasAboveDomain, xSet, xStats);
            xAxisEl.appendChild(mkXLabel(minLabel, 0, 'left'));
            xAxisEl.appendChild(mkXLabel(maxLabel, 1, 'right'));
        }

        const mkYLabel = (text, t) => {
            const el = document.createElement('div');
            el.className = 'tick-label';
            el.style.top = `${(1 - t) * 100}%`;
            el.textContent = text;
            return el;
        };

        if (isBinned) {
            const yEdgeValues = (yBinner?.edges && yBinner.edges.length === binsY + 1)
                ? yBinner.edges
                : edgeTY.map(t => invertScale(yScale.scale, t, yMin, yMax));
            yEdgeValues.forEach((value, i) => {
                const t = edgeTY[i];
                const label = formatEdgeLabel(value, i, yEdgeValues.length - 1, yHasBelowDomain, yHasAboveDomain, ySet, yStats);
                yAxisEl.appendChild(mkYLabel(label, t));
            });
        } else {
            const minLabel = formatEdgeLabel(yMin, 0, 1, yHasBelowDomain, yHasAboveDomain, ySet, yStats);
            const maxLabel = formatEdgeLabel(yMax, 1, 1, yHasBelowDomain, yHasAboveDomain, ySet, yStats);
            yAxisEl.appendChild(mkYLabel(minLabel, 0));
            yAxisEl.appendChild(mkYLabel(maxLabel, 1));
        }

        const xScaleName = String(xSet?.scale || 'linear').toLowerCase();
        const xK = xScale.scale.__exponentRaw ?? xSet?.exponent;

        const xBadge = xScaleName === 'pow'
            ? `power scale (k=${xK ?? xScale.scale.__exponent ?? 2})`
            : `${xScaleName} scale`;

        infoX.textContent = isBinned ? `${xBadge} | ${binsX}x${binsY} bins` : `${xBadge}`;
        legend.replaceChildren(this.wrapLegendForToggle(headEl, bodyEl));

        const readCssPx = (el) => {
            const r = el.getBoundingClientRect();
            return {
                w: Math.max(1, Math.round(r.width)),
                h: Math.max(1, Math.round(r.height))
            };
        };

        const render = () => {
            const { w: wrapW, h: wrapH } = readCssPx(canvasWrap);
            const cssSize = Math.max(1, Math.min(wrapW, wrapH));
            if (!Number.isFinite(cssSize) || cssSize <= 0) return;

            const pack = this.sizeCanvas(bivarCanvas, cssSize, cssSize, {
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

            const { w: xAxisW, h: xAxisH } = readCssPx(xAxisEl);
            const xAxisPack = this.sizeCanvas(xAxisCanvas, xAxisW, xAxisH, {
                style: { position: 'absolute', left: '0', top: '0' }
            });
            this.draw1DAxisCanvas({
                ctx: xAxisPack.ctx,
                crisp: xAxisPack.crisp,
                cssW: xAxisPack.cssW,
                cssH: xAxisPack.cssH,
                ticksT: isBinned ? ticksX : ticksX.map(v => xScale.scale(v)),
                tickLen: 6,
                baseline: true
            });

            const { w: yAxisW, h: yAxisH } = readCssPx(yAxisEl);
            const yAxisPack = this.sizeCanvas(yAxisCanvas, yAxisW, yAxisH, {
                style: { position: 'absolute', left: '0', top: '0' }
            });
            this.drawYAxisCanvas({
                ctx: yAxisPack.ctx,
                crisp: yAxisPack.crisp,
                cssW: yAxisPack.cssW,
                cssH: yAxisPack.cssH,
                ticksT: isBinned ? ticksY : ticksY.map(v => yScale.scale(v)),
                tickLen: 6,
                baseline: true
            });
        };

        const ro = new ResizeObserver(render);
        ro.observe(canvasWrap);
        ro.observe(xAxisEl);
        ro.observe(yAxisEl);
        window.addEventListener('resize', render, { passive: true });

        this._legendCleanup = () => {
            ro.disconnect();
            window.removeEventListener('resize', render);
        };

        render();
        this.refreshLegendChrome();
    }

    onHover(info) {
        if (!info.object || !this.currentData) {
            this.tooltip.style.display = 'none';
            this.setHoveredFeature(null);
            return;
        }

        const feature = info.object;
        this.setHoveredFeature(feature);
        this.tooltip.style.display = 'block';
        this.tooltip.style.left = info.x + 10 + 'px';
        this.tooltip.style.top = info.y + 10 + 'px';

        const tpl = document.getElementById('tooltip-template');
        const clone = tpl.content.cloneNode(true);
        const props = feature.properties || {};
        const featureId = app.dataManager.normalizeId(props.CODE ?? props.code ?? feature.id);
        const nameFromFile = app.dataManager.getFeatureNameByFeature(this.currentData, feature);
        const fallbackTitle = props?.name || featureId || feature.id;
        const title = nameFromFile || fallbackTitle || '';
        clone.querySelector('.tooltip-title').textContent = title;
        const subtitleEl = clone.querySelector('.tooltip-subtitle');
        if (subtitleEl) {
            const subtitle = featureId && featureId !== title ? featureId : '';
            subtitleEl.textContent = subtitle;
            subtitleEl.style.display = subtitle ? '' : 'none';
        }

        if (this.bivar) {
            clone.querySelector('.tooltip-metric').textContent = this.bivar.label || 'Bivariate';
            const xData = this.bivarData?.x || this.currentData;
            const yData = this.bivarData?.y || this.currentData;
            const vx = app.dataManager.getFeatureValueByFeature(xData, feature, this.bivar.x.unit);
            const vy = app.dataManager.getFeatureValueByFeature(yData, feature, this.bivar.y.unit);
            clone.querySelector('.tooltip-value').textContent = `${this.formatValue(vx, this.bivar.x.settings)} | ${this.formatValue(vy, this.bivar.y.settings)}`;
        }
        else if (this.currentMetric) {
            const v = app.dataManager.getFeatureValueByFeature(this.currentData, feature, this.currentMetric);
            clone.querySelector('.tooltip-metric').textContent = this.currentMetricLabel || this.currentMetricId || 'Metric';
            clone.querySelector('.tooltip-value').textContent = this.formatValue(v, this.currentMetricSettings);
        }

        this.tooltip.replaceChildren(clone);
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
        if (v === Infinity) return '∞';
        if (v === -Infinity) return '-∞';
        if (!Number.isFinite(v)) return 'N/A';

        const format = settings?.format || null;
        const hasExplicitFormat = !!format && typeof format === 'object';
        const useIntl = hasExplicitFormat || settings?.percentage === true;

        const cacheIntl = (options) => {
            const key = JSON.stringify(options);
            const hit = this._intlCache.get(key);
            if (hit) return hit;
            const nf = new Intl.NumberFormat(undefined, options);
            this._intlCache.set(key, nf);
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
        this.currentCategory = null;
        this.currentMetric = null;
        this.currentUnitId = null;
        this.activeComposite = null;
        this.categoricalSelections = new Map();
        this.compositeSelections = new Map();
        this.filterControls = [];
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
        this.currentCategory = category;
        this.currentData = await this.dataManager.loadCategoryData(category);
        this.updateMetricSelector();
        const cfg = this.dataManager.indexes.get('category')[category];
        const firstMetric = (cfg.metricsOrder && cfg.metricsOrder[0]) || Object.keys(cfg.metrics || {})[0];
        if (firstMetric) await this.selectMetric(firstMetric);
    }

    updateMetricSelector() {
        const container = document.getElementById('metricSelector');
        container.innerHTML = '';
        const cfg = this.dataManager.indexes.get('category')[this.currentCategory];

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

        this.currentMetric = metric;
        this.setActiveButton(`metric:${metric}`);

        const { unit, composite } = await this.resolveMetricSource(cfg, metric);
        if (!unit) return;
        this.currentUnitId = unit;
        this.activeComposite = composite;

        const stats = this.currentData ? this.dataManager.getPropertyStats(this.currentData, unit) : null;
        let catFilter = null;
        if (stats?.type === 'categorical') {
            const selection = this.ensureCategoricalSelection(unit, stats.categories);
            catFilter = new Set(selection);
        }

        this.mapRenderer.updateLayers(
            this.boundaryManager.features,
            this.currentData,
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
        this.currentMetric = null;
        this.currentUnitId = null;
        this.activeComposite = null;

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

        const xCategory = xAxis.category || this.currentCategory;
        const yCategory = yAxis.category || this.currentCategory;
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
        container.replaceChildren();
        this.filterControls = [];

        const addRangeControl = ({ label, description, onChange, formatValue }) => {
            const el = document.createElement('range-filter-control');
            container.appendChild(el);
            el.setConfig({ label, description, onChange, formatValue });
            this.filterControls.push(el);
            return el;
        };

        if (isBivar) {
            const createHandler = (axis) => (min, max) => {
                const current = this.mapRenderer.bivarFilters;
                current[axis] = { min, max };
                this.mapRenderer.updateBivariateLayers(
                    this.boundaryManager.features,
                    this.mapRenderer.bivarData,
                    this.mapRenderer.bivar,
                    this.mapRenderer.bivarCfg,
                    current
                );
            };

            const xData = this.mapRenderer.bivarData?.x || this.currentData;
            const yData = this.mapRenderer.bivarData?.y || this.currentData;
            const xCfg = this.mapRenderer.bivarCfg?.x || cfg;
            const yCfg = this.mapRenderer.bivarCfg?.y || cfg;

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
            const stats = options.stats || this.dataManager.getPropertyStats(this.currentData, unit);
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
                const currentRange = options.currentRange || this.clampFilterRange(stats, this.mapRenderer.filterRange);
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

                if (this.activeComposite && this.activeComposite.metric === metricKey) {
                    this.renderCompositeControls(container);
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
        return index[this.currentCategory] || {};
    }

    async resolveMetricSource(cfg, metric) {
        const metricDef = cfg?.metrics?.[metric] || null;
        if (!metricDef) return { unit: metric, composite: null };

        if (typeof metricDef.field === 'string') {
            await this.dataManager.ensureMetricsLoaded(this.currentCategory, [metricDef.field], this.currentData);
            return { unit: metricDef.field, composite: null };
        }

        const definition = metricDef.composite || null;
        if (!definition || !this.currentData) return { unit: metric, composite: null };

        const selection = this.getCompositeSelectionSet(metric, definition);
        const parts = (definition.parts || []).filter(part => selection.has(part));
        await this.dataManager.ensureMetricsLoaded(this.currentCategory, parts, this.currentData);
        const composite = this.dataManager.getCompositeBuffer(this.currentCategory, metric, definition, parts, this.currentData);
        if (!composite) return { unit: metric, composite: null };

        return { unit: composite.key, composite: { metric, definition, selection } };
    }

    getCompositeSelectionSet(metric, definition, categoryOverride = null) {
        const categoryKey = categoryOverride || this.currentCategory;
        const key = `${categoryKey}::${metric}`;
        let selection = this.compositeSelections.get(key);
        if (!selection) {
            const defaults = (definition.default && definition.default.length) ? definition.default : definition.parts;
            selection = new Set(defaults || []);
            this.compositeSelections.set(key, selection);
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

    applyCurrentMetric({ filterRange = this.mapRenderer.filterRange, catFilter = this.mapRenderer.categoricalFilter } = {}) {
        if (!this.currentMetric || !this.currentUnitId) return;
        const cfg = this.getCurrentCategoryConfig();
        this.mapRenderer.updateLayers(
            this.boundaryManager.features,
            this.currentData,
            this.currentUnitId,
            this.currentMetric,
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
        const key = `${this.currentCategory}::${unit}`;
        const values = categories.map(cat => cat.value);
        let selection = this.categoricalSelections.get(key);
        if (!selection) {
            selection = new Set(values);
            this.categoricalSelections.set(key, selection);
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
        const info = this.activeComposite;
        if (!info || !info.definition?.parts?.length) return;

        const panel = document.createElement('div');
        panel.className = 'composite-controls';
        panel.style.display = 'flex';
        panel.style.flexDirection = 'column';
        panel.style.gap = '6px';
        panel.style.marginTop = '12px';

        const header = document.createElement('div');
        header.style.display = 'flex';
        header.style.justifyContent = 'space-between';
        header.style.alignItems = 'center';

        const title = document.createElement('label');
        title.textContent = info.definition.label || 'Composite components';
        header.appendChild(title);

        const actions = document.createElement('div');
        actions.style.display = 'flex';
        actions.style.gap = '6px';
        const makeBtn = (label, parts) => {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.textContent = label;
            btn.className = 'btn-small';
            btn.onclick = () => { void this.updateCompositeSelection(parts); };
            return btn;
        };
        actions.appendChild(makeBtn('All', info.definition.parts));
        actions.appendChild(makeBtn('None', []));
        if (info.definition.default?.length) {
            actions.appendChild(makeBtn('Default', info.definition.default));
        }
        header.appendChild(actions);
        panel.appendChild(header);

        const grid = document.createElement('div');
        grid.style.display = 'grid';
        grid.style.gridTemplateColumns = 'repeat(2, minmax(0, 1fr))';
        grid.style.gap = '4px';

        info.definition.parts.forEach(part => {
            const row = document.createElement('label');
            row.style.display = 'flex';
            row.style.alignItems = 'center';
            row.style.gap = '6px';
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
            grid.appendChild(row);
        });

        panel.appendChild(grid);
        container.appendChild(panel);
    }

    async updateCompositeSelection(partsOverride) {
        if (!this.activeComposite) return;
        const { definition, selection, metric } = this.activeComposite;
        if (Array.isArray(partsOverride)) {
            selection.clear();
            partsOverride.forEach(part => selection.add(part));
        }

        const cfg = this.getCurrentCategoryConfig();
        const parts = (definition.parts || []).filter(part => selection.has(part));
        await this.dataManager.ensureMetricsLoaded(this.currentCategory, parts, this.currentData);
        const composite = this.dataManager.getCompositeBuffer(this.currentCategory, metric, definition, parts, this.currentData);
        if (!composite) return;

        this.currentUnitId = composite.key;
        const stats = this.dataManager.getPropertyStats(this.currentData, composite.key);
        const range = this.clampFilterRange(stats, this.mapRenderer.filterRange);

        this.mapRenderer.updateLayers(
            this.boundaryManager.features,
            this.currentData,
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
        this._configured = false;

        this.onChange = () => {};
        this.formatValue = v => v;
        this.toDisplay = v => v;
        this.fromDisplay = v => v;

        this.stats = null;
        this.settings = null;
        this.scaleObj = null;
        this.normalizeValue = null;

        this.domain = { min: 0, max: 1 };
        this.hasBelowDomain = false;
        this.hasAboveDomain = false;

        this.dom = null;
        this._handlers = null;
        this._axisCanvas = null;
        this._axisLabels = null;
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

        let dmin = Number(rawDomain?.[0]);
        let dmax = Number(rawDomain?.[rawDomain.length - 1]);
        if (!Number.isFinite(dmin)) dmin = stats.min;
        if (!Number.isFinite(dmax)) dmax = stats.max;
        if (dmin > dmax) [dmin, dmax] = [dmax, dmin];

        this.domain = { min: dmin, max: dmax };

        const eps = (Number.isFinite(stats?.max) && Number.isFinite(stats?.min))
            ? Math.max(1e-12, (stats.max - stats.min) * 1e-9)
            : 1e-12;
        this.hasBelowDomain = Number.isFinite(stats?.min) && (stats.min < dmin - eps);
        this.hasAboveDomain = Number.isFinite(stats?.max) && (stats.max > dmax + eps);

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
            const left = (lo <= 0 && this.hasBelowDomain)
                ? `≤ ${this.formatValue(this.domain.min)}`
                : this.formatValue(vMin);

            const right = (hi >= 1 && this.hasAboveDomain)
                ? `≥ ${this.formatValue(this.domain.max)}`
                : this.formatValue(vMax);

            this.dom.valDisplay.textContent = `${left} – ${right}`;
        }
    }
    #renderAxis(dmin, dmax) {

        const axis = this.dom.axis;
        if (!axis) {
            return;
        }

        const dpr = window.devicePixelRatio || 1;
        const cssW = Math.max(1, Math.round(axis.clientWidth || 220));
        const cssH = Math.max(1, Math.round(axis.clientHeight || 14));

        axis.style.position = axis.style.position || 'relative';

        if (!this._axisCanvas) {
            axis.replaceChildren();

            const canvas = document.createElement('canvas');
            canvas.style.position = 'absolute';
            canvas.style.left = '0';
            canvas.style.top = '0';
            axis.appendChild(canvas);

            const minLabel = document.createElement('div');
            minLabel.className = 'tick-label';
            axis.appendChild(minLabel);

            const maxLabel = document.createElement('div');
            maxLabel.className = 'tick-label';
            axis.appendChild(maxLabel);

            this._axisCanvas = canvas;
            this._axisLabels = { min: minLabel, max: maxLabel };
        }

        const canvas = this._axisCanvas;
        canvas.style.width = `${cssW}px`;
        canvas.style.height = `${cssH}px`;
        canvas.width = Math.max(1, Math.round(cssW * dpr));
        canvas.height = Math.max(1, Math.round(cssH * dpr));

        const ctx = canvas.getContext('2d');
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

        const crisp = (x) => (Math.round(x * dpr) + 0.5) / dpr;

        const majorCount = Math.max(2, Number(this.settings?.legendSteps || 6));
        let ticks = d3.range(majorCount).map(i => dmin + (i / (majorCount - 1)) * (dmax - dmin));
        ticks[0] = dmin;
        ticks[ticks.length - 1] = dmax;
        ticks = ticks.map(Number).filter(Number.isFinite);

        // baseline
        const y0 = crisp(2);
        ctx.strokeStyle = 'rgba(0,0,0,0.35)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(0, y0);
        ctx.lineTo(cssW, y0);
        ctx.stroke();

        // tick marks
        const tickH = 6;
        ctx.strokeStyle = 'rgba(0,0,0,0.55)';
        ctx.beginPath();
        ticks.forEach((v) => {
            const t = this.#tOf(v);
            if (t < 0 || t > 1) return;
            const x = crisp(t * (cssW - 1));
            ctx.moveTo(x, y0);
            ctx.lineTo(x, y0 + tickH);
        });
        ctx.stroke();

        // labels (endpoints only) in DOM
        const eps = (Number.isFinite(this.stats?.max) && Number.isFinite(this.stats?.min))
            ? Math.max(1e-12, (this.stats.max - this.stats.min) * 1e-9)
            : 1e-12;
        const hasBelowDomain = Number.isFinite(this.stats?.min) && (this.stats.min < dmin - eps);
        const hasAboveDomain = Number.isFinite(this.stats?.max) && (this.stats.max > dmax + eps);

        const minLabel = this._axisLabels?.min;
        const maxLabel = this._axisLabels?.max;
        if (minLabel) {
            minLabel.style.left = '0%';
            minLabel.textContent = hasBelowDomain ? `≤ ${this.formatValue(dmin)}` : this.formatValue(dmin);
        }
        if (maxLabel) {
            maxLabel.style.left = '100%';
            maxLabel.textContent = hasAboveDomain ? `≥ ${this.formatValue(dmax)}` : this.formatValue(dmax);
        }
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
