
const _ = require('lodash')
const DefaultBindings = require('./src/commands')

/**
 * Sets the root context of the filesystem. In
 * AWS that would be the bucket.
 *
 * Usage:
 * const sourceFs = rfs({bucket: 'something'})
 */
module.exports = function RemoteFileSystemFactory (options) {
  // Bucket is private and cannot be change after instantiation
  const root = options.context

  return _.extend({
    getContext () {
      return root
    }
  }, DefaultBindings())
}
