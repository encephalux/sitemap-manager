
class Queue {
    constructor() {
        this.queue = Promise.resolve();
    }

    enqueue (_task) {
        return new Promise((_reso, _rej) =>{
            this.queue = this.queue.then(_task).then(_reso).catch(_rej);
        });
    }
}

module.exports = Queue;