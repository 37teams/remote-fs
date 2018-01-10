
const Path = require('path')
const AWS = require('aws-sdk')
const s3s = require('s3-streams')
const _ = require('lodash')
const Vinyl = require('vinyl')
const through2 = require('through2')

// TODO: add sync interface
// const deasync = require('deasync')

const S3g = require('./s3g')
const vinylStream = require('./vinyl-stream')

// Method extensions
const CopyFilesTo = function () {
  const proto = {
    copyFilesTo (files, dest, options) {
      const fileStreams = files.map((file) => {
        const self = this

        return new Promise(function (resolve, reject) {
          const source = self.createReadStream(file.path, {
            delegateContext: file.bucket !== self.getContext() ? file.bucket : null
          })
          const destination = self.createWriteStream(Path.join(dest, file.relative))

          source
            .on('open', function open (object) {
              console.log('Downloading', object.ContentLength, 'bytes.')
            })
            .on('error', function error (err) {
              console.error('Unable to download file:', err)
              reject(err)
            })

          destination
            .on('error', (err) => reject(err))
            .on('finish', () => resolve())

          source.pipe(destination)
        })
      })

      return Promise.all(fileStreams)
    }
  }

  return Object.create(proto)
}

const ListFiles = function () {
  const proto = {
    listFiles (glob, options) {
      const bucket = options.delegateContext || this.getContext()
      const prefix = options.base
      const s3Params = {Bucket: bucket, Prefix: prefix}

      return new Promise(function (resolve, reject) {
        this.s3.listObjectsV2(s3Params, (err, data) => {
          if (err) {
            console.log(err, err.stack)
            reject(err)
          }

          const prefix = data.Prefix

          const files = data.Contents.map((file) => {
            return new Vinyl({
              bucket: bucket,
              key: file.Key,
              base: prefix,
              path: file.Key,
              stat: {
                mtime: new Date(file.LastModified),
                size: parseInt(file.ContentLength, 10)
              }
            })
          })

          resolve(files)
        })
      })
    }
  }

  return Object.create(proto)
}

const StreamContent = function () {
  const proto = {
    createWriteStream (path, options = {}) {
      return new s3s.WriteStream(this.bindings, { Bucket: options.delegateContext || this.getContext(), Key: path })
    },

    createReadStream (path, options = {}) {
      return new s3s.ReadStream(this.bindings, { Bucket: options.delegateContext || this.getContext(), Key: path })
    },

    createWriteThroughStream (destinationPath, options = {}) {
      const self = this

      return through2.obj(function (file, encoding, next) {
        const opts = {
          Bucket: options.delegateContext || self.getContext(),
          Key: Path.join(destinationPath, file.path)
        }
        const upload = s3s.WriteStream(self.bindings, opts)

        upload.write(file.contents)
        upload.end()

        upload.on('error', (err) => next(err))
        upload.on('finish', () => next())
      })
    }
  }

  return Object.create(proto)
}

const ReadFile = function () {
  const proto = {
    readFile (path, options) {
      const download = this.createReadStream(path)

      return new Promise(function (resolve, reject) {
        download.on('error', (err) => {
          reject(err)
        })
        download.on('end', () => {
          resolve()
        })

        // Trigger read stream
        download.read(0)
      })
    },

    readFileSync () {

    }
  }

  return Object.create(proto)
}

const WriteFile = function () {
  const proto = {
    writeFile (path, data) {
      const upload = this.createWriteStream(path)

      if (data) upload.write(data)

      upload.end()

      return new Promise(function (resolve, reject) {
        upload.on('error', (err) => {
          reject(err)
        })
        upload.on('finish', () => {
          resolve()
        })
      })
    }
  }

  return Object.create(proto)
}

const MakeDirectory = function () {
  const proto = {
    mkdir (dir) {
      const lastChar = dir.slice(-1)
      const path = (lastChar !== '/') ? `${dir}/` : dir
      return this.writeFile(path)
    }
  }

  return Object.create(proto)
}

const StreamSource = function () {
  const proto = {
    src (globs, options = {}) {
      const self = this

      function expectedError (error) {
        return (error.code === 304 || error.code === 412)
      }

      const readStream = through2.obj(function (object, encoding, next) {
        const { Key } = object

        function resolve (err, result) {
          if (err) {
            console.log('error reading src stream')
            return next(!expectedError(err) ? err : null)
          } else {
            return next(null, _.assign({ }, object, result))
          }
        }

        new s3s.ReadStream(self.bindings, { Bucket: self.getContext(), Key })
          .on('open', function open (headers) {
            resolve(null, _.assign(headers, { Body: this }))
          })
          // Handle error
          .on('error', resolve)
          // Trigger initial read for open event
          .read(0)
      })

      const s3g = new S3g(globs, {
        s3: this.bindings,
        bucket: this.getContext(),
        base: options.base || ''
      })

      return s3g.pipe(readStream).pipe(vinylStream(options))
    }
  }

  return Object.create(proto)
}

const S3Bindings = function () {
  // Expose for easy duck typing
  this.bindings = this.s3 = new AWS.S3()

  // Expose public api
  return _.extend(
    this,
    ListFiles.call(this),
    StreamContent.call(this),
    ReadFile.call(this),
    WriteFile.call(this),
    MakeDirectory.call(this),
    CopyFilesTo.call(this),
    StreamSource.call(this)
  )
}

module.exports = S3Bindings
