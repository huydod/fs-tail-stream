var fs = require('fs');
var stream = require('stream');
var once = require('once');

module.exports = createReadStream;
module.exports.createReadStream = createReadStream;

function createReadStream (path, options) {
  var ds = new stream.Duplex({ objectMode: true });
  options.autoClose = false;
  var tail = options && !options.end && options.tail;
  // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3/ManagedUpload.html#minPartSize-property
  var partSize = (options && options.partSize) || (5 * 1024 * 1024);
  var bytesRead = 0;
  var chunks = [];
  var chunksLength = 0;
  
  var close = function () {
  };

  ds.close = function () {
    close();
  };
  ds._read = once(function () {
    var rs = fs.createReadStream(path, options);
    var chunkSize = 64 * 1024;
    var pos = 0;
    var watcher;
    var interval;
    var reading = false;
    var watching = false;
    var synced = false;
    rs
      .once('open', function () {
        chunkSize = rs._readableState.highWaterMark;
        if (tail) {
          watcher = fs.watch(path, function (eventType, fileName) {
            // only kick of a read if already hit the end of the file
            ds.emit('change');
            console.log(new Date(), eventType, fileName)
            if (!reading && synced) {
              console.log(new Date(), 'read last chunk')
              reading = true;
              readChunk();
            }
          });

          interval = setInterval(() => {
            if (!reading && synced) {
              reading = true;
              readChunk();
            }
          }, 1000)

          watching = true;
          close = function () {
            watching = false;
            watcher.close();
            clearInterval(interval);
            if (!reading && synced) {
              pushChunk(null);
            }
            console.log(new Date(), 'close invoked', reading, synced)
          };
        }
      })
      .once('end', function () {
        pos = bytesRead + (options && options.start || 0);
        if (!tail) {
          ds.push(null);
        }
        synced = true;
        ds.emit('sync')
        // console.log(new Date(), 'sync', bytesRead, pos)
      })
      .pipe(ds, { end: false });

    function readChunk () {
      var b = Buffer.alloc(chunkSize);
      fs.read(rs.fd, b, 0, chunkSize, pos, function (err, bytesRead) {
        if (err) return ds.emit('error', err);
        // console.log(new Date(), pos, bytesRead)
        if (bytesRead) {
          pos += bytesRead;
          var data = b.slice(0, bytesRead);
          if (options.encoding) {
            data = data.toString(options.encoding);
          }

          pushChunk(data)
          setImmediate(readChunk);
        } else if (reading) {
          reading = false;
          ds.emit('eof')
          if (!watching) {
            ds.push(null);
          }
        }
      });
    }
  });

  ds._write = function (data, enc, cb) {
    bytesRead += Buffer.byteLength(data);
    // console.log(new Date(), 'write', data ? '' : data)
    pushChunk(data)
    cb();
  };

  function pushChunk(chunk) {
    if (chunk) {
      chunks.push(chunk);
      chunksLength += chunk.length
    }

    if (chunksLength >= partSize || chunk === null) {
      // console.log(new Date(), `Push ${chunksLength} bytes of data`)
      ds.push(Buffer.concat(chunks));
      chunks = [];
      chunksLength = 0;

      if (chunk === null) {
        ds.push(null)
      }
    }
  }

  return ds;
}
