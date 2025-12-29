import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const projectRoot = path.resolve(__dirname, '..');

export function resolveOutDir(outDir) {
  return path.resolve(projectRoot, outDir || 'dist');
}

export function createBuildOptions({ outDir, minify, sourcemap, define }) {
  return {
    entryPoints: [path.join(projectRoot, 'src/js/main.js')],
    bundle: true,
    format: 'esm',
    platform: 'browser',
    target: ['es2020'],
    sourcemap,
    minify,
    outdir: outDir,
    entryNames: 'src/js/[name]',
    define,
  };
}

async function copyDir(src, dest) {
  await fs.mkdir(path.dirname(dest), { recursive: true });
  await fs.cp(src, dest, { recursive: true });
}

async function copyFile(src, dest) {
  await fs.mkdir(path.dirname(dest), { recursive: true });
  await fs.copyFile(src, dest);
}

export function copyStaticPlugin({ outDir }) {
  const staticDirs = ['src/css', 'src/boundaries', 'src/data', 'src/img'];
  const staticFiles = ['index.html'];
  const devBundleRef = '<script type="module" src="./dist/src/js/main.js"></script>';
  const prodBundleRef = '<script type="module" src="./src/js/main.js"></script>';

  return {
    name: 'copy-static',
    setup(build) {
      build.onEnd(async (result) => {
        if (result.errors.length) return;

        const copyWork = [
          ...staticDirs.map((dir) =>
            copyDir(path.join(projectRoot, dir), path.join(outDir, dir))
          ),
        ];

        for (const file of staticFiles) {
          if (file === 'index.html') {
            const srcPath = path.join(projectRoot, file);
            const destPath = path.join(outDir, file);
            const raw = await fs.readFile(srcPath, 'utf8');
            const rewritten = raw.replace(devBundleRef, prodBundleRef);
            await fs.mkdir(path.dirname(destPath), { recursive: true });
            await fs.writeFile(destPath, rewritten, 'utf8');
            continue;
          }
          copyWork.push(
            copyFile(path.join(projectRoot, file), path.join(outDir, file))
          );
        }

        await Promise.all(copyWork);
      });
    },
  };
}
