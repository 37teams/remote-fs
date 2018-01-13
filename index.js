
const Path = require('path')
const _ = require('lodash')
const through2 = require('through2')
const S3FS = require('s3fs')

const S3g = require('./src/s3g')
const vinylStream = require('./src/vinyl-stream')

function s3fsFactory (bucket, options) {
  return new S3FS(bucket, options)
}

/**
 * Sets the root context of the filesystem. In
 * AWS that would be the bucket.
 *
 * Usage:
 * const sourceFs = rfs({context: 'something'})
 */
module.exports = function RemoteFileSystemFactory (options) {
  // Bucket is private and cannot be change after instantiation
  const root = options.context

  const bindings = options.binding
    ? options.bindings(root, options)
    : s3fsFactory(root, options)

  function expectedError (error) {
    return (error.code === 304 || error.code === 412)
  }

  function getContext () {
    return root
  }

  function writeFile (path, data) {
    return bindings.writeFile(path, data)
  }

  function mkdir (path) {
    return bindings.mkdir(path)
  }

  function src (globs, options = {}) {
    const s3g = S3g(globs, {
      bucket: getContext(),
      base: options.base || ''
    })

    const readFile = through2.obj(function (object, encoding, next) {
      bindings.readFile(object.Key, function (err, data) {
        if (err) {
          return next(!expectedError(err) ? err : null)
        }

        next(null, _.assign({ }, object, {Body: data}))
      })
    })

    return s3g.pipe(readFile).pipe(vinylStream(options))
  }

  function readFile (path, options) {
    return bindings.readFile(path, options)
  }

  function readdirp (path) {
    return bindings.readdirp(path)
  }

  function createReadStream (path, options) {
    return bindings.createReadStream(path, options)
  }

  function createWriteStream (path, options) {
    return bindings.createWriteStream(path, options)
  }

  function createWriteThroughStream (path, options) {
    return through2.obj(function (file, encoding, next) {
      writeFile(Path.join(path, file.path), file.contents)
        .then(() => next())
        .catch((err) => next(err))
    })
  }

  return {
    createReadStream,
    createWriteStream,
    createWriteThroughStream,
    getContext,
    writeFile,
    readFile,
    readdirp,
    mkdir,
    src
  }
}
