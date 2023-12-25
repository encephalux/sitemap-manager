const { open } = require("sqlite");
const sql3 = require("sqlite3");
const fs_p = require('fs/promises');
const path = require("path");
const env = require("./env");
const shared = {
    main_db: null
};

/**
 * @param _filename
 * @returns {Promise<Database>}
 */
const open_db = _filename =>
    open({
        filename : _filename,
        driver : sql3.cached.Database
    }).catch(_err => {
        throw {code: "DB_ERROR"};
    });

/**
 * @returns {Promise<Database>}
 */
const load = () =>
    open_db(path.join(env.STORAGE, "sitemap-manager.db"))
        .then(_db => shared.main_db = _db)
        .catch(_err => {
            throw {code: "MAIN_DB_ERROR"};
        });

/**
 *
 */
const check_loaded = () => {
    if(shared.main_db === null) throw "SITEMAP_MANAGER_NOT_LOADED_ERROR";
};

/**
 * @returns {Promise<void>}
 */
const init = async () => {
    // { Create storage dir }
    await fs_p.mkdir(env.STORAGE, {recursive: !0});

    // { Initialize main db }
    await open_db(path.join(env.STORAGE, "sitemap-manager.db")).then(_main_db => {
        shared.main_db = _main_db;
    });

    // { Create domains table }
    await shared.main_db.run(`
        create table t_spaces (
            _key varchar(255) not null primary key,
            _domain_name varchar(255) not null,
            _root_url text not null default '' unique,
            _urls_count bigint not null default 0,
            _parts_count bigint not null default 0,
            _size bigint not null default 0,
            _map_type varchar(3) check(_map_type in ('txt', 'xml')) not null,
            _inserted_at datetime not null default (datetime('now')),
            _updated_at datetime not null default (datetime('now'))
        )
    `);
};

/**
 * @param _date
 * @returns {string}
 */
const format_date = (_date = new Date()) => {
    const month = _date.getMonth() + 1;
    const day = _date.getDate();
    const hours = _date.getHours();
    const minutes = _date.getMinutes();
    const seconds = _date.getSeconds();

    return `${_date.getFullYear()}-${month < 10 ? "0" : ""}${month}-${day < 10 ? "0" : ""}${day} ${hours < 10 ? "0" : ""}${hours}:${minutes < 10 ? "0" : ""}${minutes}:${seconds < 10 ? "0" : ""}${seconds}`;
};

module.exports = { init, open_db, shared, load, format_date, check_loaded };