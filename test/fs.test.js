
const Code = require('code')
const Lab = require('lab')

const lab = exports.lab = Lab.script()
const describe = lab.describe
const it = lab.it
const expect = Code.expect

process.env.AWS_ACCESS_KEY_ID = 'AKIAJ4DGFCPAL7W7E25Q'
process.env.AWS_SECRET_ACCESS_KEY = '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'

const Rfs = require('..')

describe('build_pipeline', async function () {
  it('copy dir from one bucket to another', { timeout: 8888 }, async () => {
    const sourceFs = Rfs({context: 'spaces.templates'})
    const destinationFs = Rfs({context: 'gum-spaces-source'})

    const genesisFiles = await sourceFs.readdirp('/genesis')
    expect(genesisFiles).to.be.exist()

    const fileReads = genesisFiles.map((file) => {
      return sourceFs
        .readFile(`/genesis/${file}`)
        .then((result) => Promise.resolve(Object.assign(result, {Key: `/genesis/${file}`})))
    })

    Promise.all(fileReads)
      .then((files) => {
        expect(files).to.be.exist()
        const uploads = files.map((file) => {
          const relativePath = file.Key.replace('/genesis', '')
          const path = `/helloworld/master/${relativePath}`
          destinationFs.writeFile(path, file.Body)
        })
        return Promise.all(uploads)
      })
      .then(() => {
        expect(true).to.be.exist()
      })
      .catch((err) => {
        console.log(err)
        Code.fail()
      })
  })
})
