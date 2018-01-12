
const Path = require('path')
const AWS = require('aws-sdk')
const s3s = require('s3-streams')
const _ = require('lodash')
const Vinyl = require('vinyl')
const through2 = require('through2')
const deasync = require('deasync')
const buffer = require('vinyl-buffer')
const requireFromString = require('require-from-string')

const S3g = require('./s3g')
const vinylStream = require('./vinyl-stream')

// Method extensions
const CopyFilesTo = {
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

const ListFiles = {
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

const StreamContent = {
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

// TODO: move to its own package
const RequireGlob = {
  requireGlob: function (globs, options) {
    const modules = {}

    console.log({globs})

    const requireS3 = through2.obj(function (file, encoding, next) {
      console.log({file})
      modules[file.relative] = requireFromString(file.content.toString('utf-8'))
      next()
    })

    return new Promise((resolve, reject) => {
      this.src(globs, options = {})
      .pipe(buffer())
      .pipe(requireS3)
      .on('error', (err) => {
        console.log('Error requiring glob')
        reject(err)
      })
      .on('finish', () => {
        console.log('Finished requiring glob')
        console.log(modules)
        resolve(modules)
      })
      .on('close', () => {
        console.log('Closing requiring glob')
        //resolve(modules)
      })
    })
  },

  requireGlobSync: function (globs, options) {
    console.log('require glob sync.....')
    let done = false
    let modules = {}

    this.requireGlob(globs, options)
      .then((result) => {
        console.log('require globbing done')
        done = true
        modules = result
      })
      .catch((err) => {
        console.log(err)
        done = true
      })

    deasync.loopWhile(function () {
      return !done
    })

    // Content is ready to return
    return modules
  }
}

const ReadFile = {
  readFile (path, options) {
    let fileContent = null

    const download = this.createReadStream(path)

    return new Promise(function (resolve, reject) {
      download.on('error', (err) => {
        reject(err)
      })
      download.on('end', () => {
        resolve(fileContent)
      })

      // Trigger read stream
      download.on('data', (chunk) => {
        console.log('getting data from s3 file')
        fileContent =  chunk.toString('utf8')
      })
    })
  },

  readFileSync (path, options) {
    let done = false
    let fileContent = null

    this.readFile(path, options)
      .then((content) => {
        fileContent = content
        done = true
      })

    // Block without blocking full thread
    deasync.loopWhile(function () {
      return !done
    })

    // Content is ready to return
    return fileContent
  }
}

const WriteFile = {
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

const MakeDirectory = {
  mkdir (dir) {
    const lastChar = dir.slice(-1)
    const path = (lastChar !== '/') ? `${dir}/` : dir
    return this.writeFile(path)
  }
}

const StreamSource = {
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

const S3Bindings = function () {
  // Expose for easy duck typing
  this.bindings = this.s3 = new AWS.S3()

  // Expose public api
  return _.extend(
    this,
    ListFiles,
    StreamContent,
    ReadFile,
    WriteFile,
    MakeDirectory,
    CopyFilesTo,
    StreamSource,
    RequireGlob
  )
}

module.exports = S3Bindings
