const db = require('./db.js')
const ReactiveDao = require("reactive-dao")

class RethinkObservableValue extends ReactiveDao.ObservableValue {

  constructor(request) {
    super()

    this.request = request
    this.disposed = false

    db().then(conn => {
      if(this.disposed) return;
      request.changes({ includeInitial: true }).run(conn).then(
        changesStream => {
          this.changesStream = changesStream
          if(this.disposed) {
            changesStream.close()
            return false
          }
          changesStream.each((err, change) => {
            if(this.disposed) {
              changesStream.close()
              return false
            }
            if(err) {
              changesStream.close()
              return false
            }
            this.set(change.new_val)
          })
        }
      )
    })

  }
  
  dispose() {
    this.disposed = true
    if(this.changesStream) this.changesStream.close()
  }

}

module.exports = RethinkObservableValue