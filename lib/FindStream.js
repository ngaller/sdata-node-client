const { Readable } = require('stream')

class FindStream extends Readable {
  constructor(request, startUrl, limit = 0) {
    super({objectMode: true})
    this.request = request
    this.startUrl = startUrl
    this.limit = limit
    this.numRetrieved = 0
  }

  _read(n) {
    if(!this.isReading) {
      this.isReading = true
      this.startReading()
    }
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
        if(body.$next) {
          this.startUrl = body.$next
          setImmediate(() => this.startReading())
        } else {
          this.push(null)
        }
      }, err => {
        this.emit('error', new Error('Error reading records: ' + err.toString()))
        // we can't read any more, since we don't have a next link!  So end the stream
        this.push(null)
      })
  }
}

module.exports = FindStream