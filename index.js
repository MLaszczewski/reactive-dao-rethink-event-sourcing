const r = require('rethinkdb')
const ReactiveDao = require("reactive-dao")

const db = require('./lib/db.js')
const RethinkObservableValue = require('./lib/RethinkObservableValue.js')
const RethinkObservableList = require('./lib/RethinkObservableList.js')

function command(service, command, parameters) {
  let conn
  return db().then(connection => {
    conn = connection
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
    return r.table( service + '_commands' ).get(commandId).changes({ includeInitial: true  }).run(conn)
  }).then( changesStream => new Promise( (resolve, reject) => {
    changesStream.each( (err, result) => {
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

function getValue(requestPromise) {
  return Promise.all([db(), requestPromise]).then(([conn, request]) => request.run(conn))
}
function observableValue(requestPromise) {
  return new RethinkObservableValue(requestPromise)
}
function simpleValue(requestCallback) {
  return {
    get: (...args) => getValue( requestCallback('get', ...args).run() ),
    observable: (...args) => observableValue( requestCallback('observe', ...args) )
  }
}

function getList(requestPromise) {
  return Promise.all([db(), requestPromise]).then(([conn, request]) => request.run(conn))
}
function observableList(requestPromise, idField) {
  return new RethinkObservableList(requestPromise)
}
function simpleList(requestCallback, idField) {
  return {
    get: (...args) => getList( requestCallback('get', ...args).run() ),
    observable: (...args) => observableList( requestCallback('observe', ...args), idField )
  }
}


module.exports = {
  
  connnectToDatabase: db,
  command,

  getValue,
  observableValue,
  simpleValue,

  getList,
  observableList,
  simpleList

}