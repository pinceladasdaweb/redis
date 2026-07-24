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
  // A library build must never inline dependencies: everything that is not
  // the library's own source (deps, node builtins) stays external.
  external: (id) => !id.startsWith('.') && !id.startsWith('/')
}
