const path = require("path");
const fs_p = require("fs/promises");
const Queue = require("./Queue");
const {check_loaded} = require("./core");
const Space = require("./Space");
const env = require("./env");

class TxtSpace extends Space {
    queue = new Queue();

    async register_url(_url) {
        check_loaded();

        return this.queue.enqueue(async () => {
            const part_item = this.part_item(_url);
            const size = Buffer.from(part_item).length;

            const result = await this.db.get(`select _number from t_parts where _urls_count <= $urls_count_limit and (_size + $item_size) <= $size_limit`, {
                '$urls_count_limit': env.LIMITS.URL_COUNT,
                '$size_limit': env.LIMITS.FILE_SIZE,
                '$item_size': size
            });

            const part = result ? result._number : await this.init_part();

            await super.register_url({
                _url: _url,
                _part: part,
                _size: size
            });

            await fs_p.writeFile(path.join(this.dir, `sitemap-${part}.txt`), part_item, {flag: "a"});
        });
    }

    async update_url({
        _url,
        _new_url
    }) {
        if(!_new_url || _new_url === _url) return;

        check_loaded();

        return this.queue.enqueue(async () => {
            const {_part: part} = await this.db.get(`select _part from t_urls where _url = ?`, [_url]);

            await this.#part_process({
                _part: part,
                _processor: (_obj) => _obj._tmp_fd.write(this.part_item(_obj._url === _url ? _new_url : _obj._url))
            });

            await super.update_url({
                _url,
                _new_url,
                _part: part
            });
        });
    }

    async delete_url({_url, _size = Buffer.from(this.part_item(_url)).length}) {
        check_loaded();

        return this.queue.enqueue(async () => {
            const {_part: part} = await this.db.get(`select _part from t_urls where _url = ?`, [_url]);

            await this.#part_process({
                _part: part,
                _processor: async (_obj) => {
                    if(_obj._url === _url) return;

                    await _obj._tmp_fd.write(this.part_item(_obj._url));
                }
            });

            await super.delete_url({_url, _part: part});
        });
    }

    async init_part() {
        check_loaded();

        return this.queue.enqueue(async () => {
            const number = await super.init_part();
            await fs_p.writeFile(path.join(this.dir, `sitemap-${number}.txt`), "");

            return number;
        });
    }

    part_item(_url = "") {
        return _url + "\n";
    }

    async gen_index() {
        if(this._parts_count === 1) return;

        let xml = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`;
        for(let i=1; i <= this._parts_count; i++) {
            xml += `
    <sitemap>
        <loc>${this._root_url}/sitemap-${i}.txt</loc>
        <lastmod>${(await this.db.get(`select _updated_at from t_parts where _number = ?`, [i]))._updated_at}</lastmod>
    </sitemap>
`;
        }

        xml += `</sitemapindex>`;

        await fs_p.writeFile(path.join(this.dir, "sitemap.txt"), xml);
    }

    async #part_process({_part, _processor = async ({_url, _old_fd, _tmp_fd}) => null}) {
        const sitemap_path = path.join(this.dir, `sitemap-${_part}.txt`);
        const tmp_path = path.join(this.dir, `sitemap-${_part}.txt.tmp`);

        const old_fd = await fs_p.open(sitemap_path, "r");
        const tmp_fd = await fs_p.open(tmp_path, "w");

        for await(const url of old_fd.readLines()) {
            await _processor({
                _url: url,
                _old_fd: old_fd,
                _tmp_fd: tmp_fd
            });
        }

        await old_fd?.close();
        await tmp_fd?.close();

        await fs_p.rename(tmp_path, sitemap_path);
    }

    static async register({
        _key,
        _domain_name,
        _root_url
    }) {
        check_loaded();

        const space =  new TxtSpace(await super.register({
            _key,
            _domain_name,
            _root_url,
            _map_type: "txt"
        }));

        await space.register_url(_root_url);

        return space;
    }

    static async load(_key) {
        check_loaded();
        const data = await super.load(_key);

        if(data._map_type !== "txt") throw new Error("SPACE_BAD_TYPE_LOADING");

        return new TxtSpace(data);
    }
}

module.exports = TxtSpace;