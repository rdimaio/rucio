const Configuration = {
    extends: ['@commitlint/config-conventional'],
    parserPreset: 'conventional-changelog-atom',
    formatter: '@commitlint/format',
    /*
     * Any rules defined here will override rules from @commitlint/config-conventional
     */
    rules: {
      'type-enum': [2, 'always', ['foo']],
    },

    /*
     * Array of functions that return true if commitlint should ignore the given message.
     * Given array is merged with predefined functions, which consist of matchers like:
     *
     * - 'Merge pull request', 'Merge X into Y' or 'Merge branch X'
     * - 'Revert X'
     * - 'v1.2.3' (ie semver matcher)
     * - 'Automatic merge X' or 'Auto-merged X into Y'
     *
     * To see full list, check https://github.com/conventional-changelog/commitlint/blob/master/%40commitlint/is-ignored/src/defaults.ts.
     * To disable those ignores and run rules always, set `defaultIgnores: false` as shown below.
     */
    ignores: [(commit) => commit === ''],

    /*
     * Whether commitlint uses the default ignore rules, see the description above.
     */
    defaultIgnores: true,

    /*
     * Custom URL to show upon failure
     */
    helpUrl:
      'https://github.com/conventional-changelog/commitlint/#what-is-commitlint',
      
    /*
     * Custom prompt configs
     */
    prompt: {
      messages: {},
      questions: {
        type: {
          description: 'please input type:',
        },
      },
    },
  };
  
  export default Configuration;