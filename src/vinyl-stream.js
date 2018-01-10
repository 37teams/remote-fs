
const Vinyl = require('vinyl')
const through2map = require('through2-map')

module.exports = function VinylStream (options) {
  return through2map.obj(function (object) {
    const base = options.base || ''

    const file = new Vinyl({
      base: base,
      contents: object.Body,
      stat: {
        mtime: new Date(object.LastModified),
        size: parseInt(object.ContentLength, 10)
      },
      path: object.Key
    })

    // Set the length on things like streams which are used
    // by some frameworks.
    if (!file.isNull() && !file.contents.length) {
      file.contents.length = file.stat.size
    }

    // Assign AWS metadata to vinyl files
    // Support for Content-Type
    if (object.ContentType) {
      file.contentType = object.ContentType
    }

    return file
  })
}
