import rethink from 'rethinkdb';

const name = 'rethinkdb-store';

const sanitizeQueryFilters = (filters) => (
  Object.keys(filters)
    .filter((key) => !key.match(/\$$/))
    .reduce((acc, key) => Object.assign(acc, { [key]: filters[key] }), {})
);

const wrapQuery = (r, rQuery, query) => {
  const { native$, sort$, limit$, skip$, fields$ } = query;
  let finalQuery = rQuery;

  if (native$) {
    return query;
  }

  if (sort$) {
    const index = Object.keys(sort$)[0];
    const sortBy = sort$[index] < 0 ? r.desc(index) : index;
    finalQuery = finalQuery.orderBy(sortBy);
  }

  if (limit$) {
    finalQuery = finalQuery.limit(limit$);
  }

  if (skip$) {
    finalQuery = finalQuery.skip(skip$);
  }

  if (fields$) {
    finalQuery = finalQuery.pluck(...fields$);
  }

  return finalQuery;
};

export default function rethinkDbStore(opts = {}) {
  const options = Object.assign({
    autoCreateTables: true
  }, opts);

  const r = opts.r || rethink;
  const seneca = this;
  let desc = '';
  let conn;

  const handleError = (err, cb = () => {}) => {
    if (err) {
      seneca.log.error('entity', err, { store: name });
      cb(err);
    }

    return err;
  };

  const configure = (spec = {}, cb) => {
    if (spec.connection) {
      conn = spec.connection;
      return cb();
    }

    const dbopts = seneca.util.deepextend({
      host: 'localhost',
      port: 28015,
      db: 'test'
    }, spec);

    return r.connect(dbopts, (err, connection) => {
      if (err) {
        return seneca.die('connect', err, dbopts);
      }

      conn = connection;
      seneca.log.debug('init', 'connect');
      return cb();
    });
  };

  const getTable = (ent) => {
    const { base, name } = ent.canon$({ object: true }); // eslint-disable-line
    const prefix = base ? `${base}_` : '';
    const tableName = `${prefix}${name}`;
    return r.table(tableName);
  };

  const store = {
    name,

    close(args, cb) {
      return conn ? conn.close(cb) : cb();
    },

    save({ ent }, cb) {
      const table = getTable(ent);
      const isUpdate = !!ent.id;
      const data = ent.data$(false);

      if (isUpdate) {
        table.get(ent.id).update(data).run(conn, (err) => {
          if (!handleError(err, cb)) {
            cb(null, ent);
          }
        });
      } else {
        table.insert(data).run(conn, (err, result) => {
          if (!handleError(err, cb)) {
            if (Array.isArray(result.generated_keys)) {
              ent.id = result.generated_keys[0]; // eslint-disable-line no-param-reassign
            }

            cb(null, ent);
          }
        });
      }
    },

    load({ q, qent }, cb) {
      const table = getTable(qent);
      const { id } = q;

      const query = q.id ? table.get(id) : table.filter(q).limit(1);

      query.run(conn, (err, cursor) => {
        if (!handleError(err, cb)) {
          if (cursor.toArray) {
            cursor.toArray((err, results) => {
              if (!handleError(err, cb)) {
                cb(null, results.length ? qent.make$(results[0]) : null);
              }
            });
          } else {
            cb(null, cursor && qent.make$(cursor));
          }
        }
      });
    },

    list({ q, qent }, cb) {
      const table = getTable(qent);
      const filters = sanitizeQueryFilters(q);
      const query = wrapQuery(r, table.filter(filters), q);

      query.run(conn, (err, cursor) => {
        if (!handleError(err, cb)) {
          const result = [];
          cursor.each((err, item) => {
            if (!handleError(err, cb)) {
              result.push(qent.make$(item));
            }
            return !err;
          }, () => {
            cb(null, result);
          });
        }
      });
    },

    remove({ q, qent }, cb) {
      const table = getTable(qent);
      const all = q.all$;
      const load = q.load === undefined ? true : q.load$;

      const parseResult = (result) => {
        if (!load) {
          return [];
        }
        return result.changes.map((e) => qent.make$(e.old_val));
      };

      if (all) {
        table.delete({ returnChanges: load }).run(conn, (err, result) => {
          if (!handleError(err, cb)) {
            cb(null, parseResult(result));
          }
        });
      } else {
        table.filter(q).delete({ returnChanges: load }).run(conn, (err, result) => {
          if (!handleError(err, cb)) {
            cb(null, parseResult(result));
          }
        });
      }
    },

    native({ ent }, done) {
      const table = getTable(ent);
      done(null, { r, conn, table });
    }
  };


  const meta = seneca.store.init(seneca, opts, store);
  desc = meta.desc;

  seneca.add({ init: name, tag: meta.tag }, (args, done) => {
    configure(opts, (err) => {
      if (err) {
        return seneca.die('store', err, {
          store: store.name,
          desc
        });
      }

      return done();
    });
  });


  return {
    name,
    tag: meta.tag
  };
}
