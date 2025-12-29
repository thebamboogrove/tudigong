import { build } from 'esbuild';
import {
    copyStaticPlugin,
    createBuildOptions,
    resolveOutDir,
} from './esbuild.config.js';

function getArgValue(args, key) {
    const i = args.indexOf(key);
    if (i === -1) return null;
    return args[i + 1] ?? null;
}

(async () => {
    const args = process.argv.slice(2);
    const outDir = resolveOutDir(getArgValue(args, '--outdir'));

    try {
        await build({
            ...createBuildOptions({
                outDir,
                minify: true,
                sourcemap: true,
                define: {
                    'process.env.NODE_ENV': '"production"'
                },
            }),
            plugins: [copyStaticPlugin({ outDir })],
        });

        console.log(`Built to ${outDir}`);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
})();
