const { Readable } = require('stream')
const convertSDataError = require('./convertSDataError.js')

class FindStream extends Readable {
  constructor(request, startUrl, limit = 0) {
    super({objectMode: true})
    this.request = request
    this.startUrl = startUrl
    if(this.startUrl.startsWith('https://')) {
      this.ssl = true;
    }
    this.limit = limit
    this.numRetrieved = 0
  }

  _read(n) {
    if(!this.isReading) {
      this.isReading = true
      this.startReading()
    }
  }

  _destroy() {
    this.isReading = false
    this.push(null)
  }

  startReading() {
    this.request.get(this.startUrl)
      .then(({statusCode, body}) => {
        if(statusCode !== 200) {
          this.emit('error', new Error('Invalid status code ' + statusCode))
          return
        }
        for(let rec of body.$resources) {
          this.push(rec)
          this.numRetrieved++
          if(this.numRetrieved === this.limit) {
            this.push(null)
            return
          }
        }
        if(body.$next && this.isReading) {
          this.startUrl = body.$next
          if(this.ssl) {
            this.startUrl = this.startUrl.replace(/^http:/, 'https:')
          }
          setImmediate(() => this.startReading())
        } else {
          this.push(null)
        }
      }, err => {
        this.emit('error', convertSDataError(err, this.startUrl))
        // we can't read any more, since we don't have a next link!  So end the stream
        this.push(null)
      })
  }
}

module.exports = FindStream
