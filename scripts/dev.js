import { context } from 'esbuild';
import { createBuildOptions, projectRoot, resolveOutDir } from './esbuild.config.js';

function getArgValue(args, key) {
  const i = args.indexOf(key);
  if (i === -1) return null;
  return args[i + 1] ?? null;
}

(async () => {
  const args = process.argv.slice(2);
  const outDir = resolveOutDir(getArgValue(args, '--outdir'));
  const positionalPort = args.find((a) => !String(a).startsWith('-'));
  const portRaw = getArgValue(args, '--port') || positionalPort || process.env.PORT || 5173;
  const port = Number(portRaw);
  const host = getArgValue(args, '--host') || process.env.HOST || 'localhost';

  const ctx = await context(
    createBuildOptions({
      outDir,
      minify: false,
      sourcemap: true,
      define: {
        'process.env.NODE_ENV': '"development"',
      },
    })
  );

  await ctx.watch();
  const server = await ctx.serve({ servedir: projectRoot, port, host });
  console.log(`Dev server: http://${server.host || host}:${server.port}/index.html`);
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
