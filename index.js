const r = require('rethinkdb')
const ReactiveDao = require("reactive-dao")

const db = require('./lib/db.js')
const RethinkObservableValue = require('./lib/RethinkObservableValue.js')

function command(service, command, parameters) {
  return db().then(conn => {
    let cmd
    if(parameters) {
      cmd = parameters
      cmd.type = command
    } else {
      cmd = command
    }
    cmd.state = "new"
    return r.table( service + "_commands" ).insert(cmd).run(conn)
  }).then( result => {
    let commandId = result.generated_keys[0]
    return r.table(table + '_commands').get(commandId).changes({ includeInitial: true  }).run(conn)
  }).then( changesStream => new Promise( (resolve, reject) => {
    changeStream.each( (err, result) => {
      if(err) {
        changesStream.close();
        reject(err)
        return false
      }
      let val = result.new_val
      if(val.state == "done") {
        resolve(val.result)
        changesStream.close()
        return false
      }
      if(val.state == "failed") {
        reject(val.error)
        changesStream.close()
        return false
      }
    })
  }))
}

function getValue(request) {
  return db().then(conn => request.run(conn))
}

function observableValue(request) {
  return new RethinkObservableValue(request)
}

function simpleValue(requestCallback) {
  return {
    get: () => getValue( requestCallback('get').run() )
    observable: () => observableValue( requestCallback('observe') )
  }
}

module.exports = {
  
  connnectToDatabase,
  command,
  getValue,
  observableValue,
  simpleValue

}