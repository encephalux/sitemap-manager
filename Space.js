const fs_p = require("fs/promises");
const path = require("path");
const env = require("./env");
const {open_db, format_date, check_loaded, shared} = require("./core");

class Space {
    constructor({
        _key,
        _domain_name,
        _root_url,
        _map_type,
        _urls_count = 0,
        _parts_count = 0,
        _size = 0,
        _inserted_at = null,
        _updated_at = null,
        _db = null
    }) {
        check_loaded();

        this._key = _key;
        this._domain_name = _domain_name;
        this._root_url = _root_url;
        this._urls_count = _urls_count;
        this._parts_count = _parts_count;
        this._size = _size;
        this._map_type = _map_type;
        this._inserted_at = _inserted_at;
        this._updated_at = _updated_at;
        this.dir = path.join(env.STORAGE, this._key);
        this.db_path = path.join(this.dir, `${_key}.db`);
        this.db = _db;
    }

    async register_url({_url, _lastmod, _changefreq, _priority = 0.0, _part, _size = Buffer.from(this.part_item(_url)).length}) {
        // { Register the url in the index }
        await this.db.run(`insert into t_urls(_url, _lastmod, _changefreq, _priority, _part) values(?, ?, ?, ?, ?)`, {
            1: _url,
            2: _lastmod,
            3: _changefreq,
            4: _priority,
            5: _part
        });
        await shared.main_db.run(`update t_spaces set _size = _size + ?, _urls_count = _urls_count + 1, _updated_at = (datetime('now')) where _key = ?`, [_size, this._key]);
        await this.db.run(`update t_parts set _size = _size + ?, _urls_count = _urls_count + 1, _updated_at = (datetime('now')) where _number = ?`, [_size, _part]);

        return {_url, _lastmod, _changefreq, _priority, _part, _size};
    }

    async update_url({
        _url,
        _new_url,
        _lastmod,
        _changefreq,
        _priority,
        _part,
        _size = Buffer.from(this.part_item(_url)).length,
        _new_size
    }) {
        let fields = "";
        let values = {'$url': _url};
        let dsize = 0;

        if(_new_url && _new_url !== _url) {
            fields += "_url = $new_url";
            values["$new_url"] = _new_url;
            dsize = (_new_size || Buffer.from(this.part_item(_new_url)).length) - _size;
        }

        if(_lastmod) {
            fields += (fields && ", ") + "_lastmod = $lastmod";
            values["$lastmod"] = _lastmod;
        }

        if(_changefreq) {
            fields += (fields && ", ") + "_changefreq = $change_freq";
            values["$change_freq"] = _changefreq;
        }

        if(_priority) {
            fields += (fields && ", ") + "_priority = $priority";
            values["$priority"] = _priority;
        }

        if(!fields) return;

        await this.db.run(`update t_urls set ${fields}, _updated_at = datetime() where _url = $url`, values);

        if(dsize !== 0) {
            await shared.main_db.run(`update t_spaces set _size = _size + ?, _updated_at = (datetime('now')) where _key = ?`, [dsize, this._key]);
            await this.db.run(`update t_parts set _size = _size + ?, _updated_at = (datetime('now')) where _number = ?`, [dsize, _part]);
        }
    }

    async delete_url({_url, _part, _size = Buffer.from(this.part_item(_url)).length}) {
        // { Delete url from the index }
        await this.db.run(`delete from t_urls where _url = ?`, [_url]);
        await shared.main_db.run(`update t_spaces set _size = _size - ?, _urls_count = _urls_count - 1, _updated_at = (datetime('now')) where _key = ?`, [_size, this._key]);
        await this.db.run(`update t_parts set _size = _size - ?, _urls_count = _urls_count - 1, _updated_at = (datetime('now')) where _number = ?`, [_size, _part]);
    }

    async init_part() {
        // { Get the next part number }
        const number = (await shared.main_db.get(`select _parts_count from t_spaces where _key = ?`, [this._key]))._parts_count + 1;

        // { Register the new part in database }
        await this.db.run(`insert into t_parts(_number) values (?)`, [number]);
        await shared.main_db.run(`update t_spaces set _parts_count = _parts_count + 1, _updated_at = datetime() where _key = ?`, [this._key]);

        if(number > 1) await this.gen_index();

        return number;
    }

    part_item(_url) {
        return "";
    }

    async gen_index() {}

    static async register({
        _key,
        _domain_name,
        _root_url,
        _map_type
    }) {
        // { Register the space in main db }
        const insert_date = format_date();
        await shared.main_db.run(`insert into t_spaces(_key, _domain_name, _root_url, _map_type, _inserted_at) values(?, ?, ?, ?, ?)`, {
            1: _key,
            2: _domain_name,
            3: _root_url,
            4: _map_type,
            5: insert_date
        });

        // { Init space folder }
        const space_dir = path.join(env.STORAGE, _key);
        await fs_p.mkdir(space_dir, {recursive: !0});
        const space_db = await open_db(path.join(space_dir, `${_key}.db`));
        await space_db.run(`
            create table t_parts(
                _number smallint primary key,
                _urls_count smallint not null default 0,
                _size bigint not null default 0,
                _inserted_at datetime not null default (datetime('now')),
                _updated_at datetime not null default (datetime('now'))
            )
        `);
        await space_db.run(`
            create table t_urls(
                _url text not null primary key,
                _lastmod datetime,
                _changefreq varchar(30),
                _priority double not null default 0.0,
                _part smallint,
                _inserted_at datetime not null default (datetime('now')),
                _updated_at datetime not null default (datetime('now')),
                constraint fk_urls_parts foreign key (_part) references t_parts(_number)
            )
        `);

        return {
            _key,
            _domain_name,
            _root_url,
            _map_type,
            _inserted_at: insert_date,
            _db: space_db
        };
    }

    static async load(_key) {

        return {
            ...(await shared.main_db.all(`select * from t_spaces where _key = ?`, [_key]))[0],
            _db: await open_db(path.join(env.STORAGE, _key, `${_key}.db`))
        };
    }
}

module.exports = Space;