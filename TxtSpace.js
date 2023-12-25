const {check_loaded} = require("./core");
const Space = require("./Space");

class TxtSpace extends Space {

    async register_url({
        _url,
        _lastmod,
        _change_freq,
        _priority = 0.0,
        _part = null,
        _size = Buffer.from(this.part_item(_url)).length
    }) {
        check_loaded();

        await super.register_url({
            _url,
            _lastmod,
            _change_freq,
            _priority,
            _part,
            _size
        });
    }

    async update_url({
        _url,
        _new_url,
        _lastmod,
        _change_freq,
        _priority,
        _part,
        _size = Buffer.from(this.part_item(_url)).length,
        _new_size
    }) {
        check_loaded();

        await super.update_url({
            _url,
            _new_url,
            _lastmod,
            _change_freq,
            _priority,
            _part,
            _size,
            _new_size
        });
    }

    async delete_url({_url, _size = Buffer.from(this.part_item(_url)).length}) {
        check_loaded();
    }

    async init_part() {
        check_loaded();

        await super.init_part();
    }

    part_item(_url = "") {
        return _url;
    }

    static async register({
        _key,
        _domain_name,
        _url_root
    }) {
        check_loaded();

        return new TxtSpace(await super.register({
            _key,
            _domain_name,
            _url_root,
            _map_type: "txt"
        }));
    }

    static async load(_key) {
        check_loaded();
        const data = await super.load(_key);

        if(data._map_type !== "txt") throw new Error("SPACE_BAD_TYPE_LOADING");

        return new TxtSpace(data);
    }
}

module.exports = TxtSpace;