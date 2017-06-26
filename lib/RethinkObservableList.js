const db = require('./db.js')
const ReactiveDao = require("reactive-dao")

class RethinkObservableList extends ReactiveDao.ObservableList {

  constructor(requestPromise, idField) {
    super()

    this.disposed = false
    this.ready = false
    this.idField = idField

    let loadingList = []

    Promise.all([db(), requestPromise]).then(([conn, request]) => {
      if(this.disposed) return;
      request.changes({ includeInitial: true, includeStates: true }).run(conn).then(
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

            if(!this.ready) {
              if(!change.state) {
                loadingList.push(change.new_val)
              } else if(change.state == 'ready') {
                this.set(loadingList)
                this.ready = true
              }
            } else {
              if(change.state) return;
              if(change.old_val && change.new_val) {
                if(idField) this.updateByField(idField, change.old_val[idField], change.new_val)
                  else this.update(change.old_val, change.new_val)
              } else if(change.new_val) {
                this.push(change.new_val)
              } else if(change.old_val) {
                if(idField) this.removeByField(idField, change.old_val[idField])
                  else this.remove(change.old_val)
              }
            }

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

module.exports = RethinkObservableList