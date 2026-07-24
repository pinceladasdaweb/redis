import json from '@rollup/plugin-json'
import commonjs from '@rollup/plugin-commonjs'
import resolve from '@rollup/plugin-node-resolve'
export default {
  input: 'src/index.js',
  output: [
    {
      file: 'dist/index.cjs',
      format: 'cjs',
      exports: 'named'
    },
    {
      file: 'dist/index.mjs',
      format: 'es',
      exports: 'named'
    }
  ],
  plugins: [
    resolve(),
    commonjs(),
    json()
  ],
  external: ['ioredis', 'pino', 'pino-pretty']
}
