
const assert = require('assert')
const Path = require('path')
const { Readable } = require('readable-stream')
const minimatch = require('minimatch')
const _ = require('lodash')
const AWS = require('aws-sdk')

class S3g extends Readable {
  constructor (globs, options) {
    options = Object.assign({ objectMode: true }, options)

    assert(options.bucket, 'S3 bucket is required')

    super(options)

    this.s3 = options.s3 || new AWS.S3()
    this._bucket = options.bucket
    this._prefix = options.base || ''

    // Handle the single value case
    if (_.isString(globs)) globs = [ globs ]

    let { filters = [], searches = [] } = _.groupBy(globs, function (glob) {
      return _.isString(glob) && glob.charAt(0) === '!' ? 'filters' : 'searches'
    })

    this.filters = filters.map((filter) => {
      const pattern = Path.join(this._prefix, filter.slice(1))
      return (key) => minimatch(key, pattern, {dot: true})
    })

    this.searches = searches.map((search) => {
      const pattern = Path.join(this._prefix, search)
      return (key) => minimatch(key, pattern, {dot: true})
    })
  }

  _match (key) {
    return (
      _.find(this.searches, (search) => search(key)) &&
      !_.find(this.filters, (filter) => filter(key))
    )
  }

  _read (size) {
    // We have to use a kind of primitive lock since _read is called
    // constantly when pushing data.
    if (this._reading) return

    // No work needs doing if we're not reading anything.
    if (size <= 0) return

    this._reading = true

    const params = {
      Bucket: this._bucket,
      Prefix: this._prefix
    }

    this.s3.listObjectsV2(params, (err, data) => {
      if (err) {
        process.nextTick(() => this.emit('error', err))
        return
      }

      const files = data.Contents || []

      files.forEach((file) => {
        if (this._match(file.Key)) {
          this.push(file)
        }
      })

      // Unlock for more reads
      this._reading = false

      // TODO: handle truncation
      this.push(null)
    })
  }
}

module.exports = S3g
