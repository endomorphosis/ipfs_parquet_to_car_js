import parquetjs from '@dsnp/parquetjs';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { Buffer } from 'buffer';
import { Readable } from 'stream';
import { CarReader, CarWriter } from '@ipld/car';
import { CID } from 'multiformats/cid';
import * as Block from 'multiformats/block';
import * as codec from '@ipld/dag-cbor';
import { sha256 } from 'multiformats/hashes/sha2';

export class ipfsParquetToCarJs {
    constructor(resources, metadata) {
        this.resources = resources;
        this.metadata = metadata;
        this.car_archive = null;
        this.parquet_archive = null;
        this.cids = [];
        this.bytes = [];
        this.hash = [];
    }

    async convert_parquet_to_car(parquet_file, dst_path = null) {
        try {
            const this_dir = path.dirname(import.meta.url).replace('file://', '');
            let parquet_path = parquet_file;
            if (!parquet_path.startsWith('/')) {
                parquet_path = path.join(this_dir, parquet_path);
            }
            if (!dst_path) {
                dst_path = parquet_path.replace('.parquet', '.car');
            } else if (!dst_path.startsWith('/')) {
                dst_path = path.join(this_dir, dst_path);
            }
            const parquet = await parquetjs.ParquetReader.openFile(parquet_path);
            const cursor = parquet.getCursor();
            let record = null;
            while ((record = await cursor.next())) {
                // Encode the record using dag-cbor
                const block = await Block.encode({
                    value: record,
                    codec: codec,
                    hasher: sha256,
                });
                this.bytes.push(block.bytes);
                this.cids.push(block.cid);
            }
            // Create the CAR file
            let { writer, out } = await CarWriter.create([this.cids[0]]);
            if (dst_path) {
                Readable.from(out).pipe(fs.createWriteStream(dst_path));
            } else {
                Readable.from(out).pipe(
                    fs.createWriteStream(parquet_path.replace('.parquet', '.car'))
                );
            }
            const len_cids = this.cids.length;
            for (let i = 0; i < len_cids; i++) {
                let this_cid = this.cids[i];
                let this_bytes = this.bytes[i];
                await writer.put({ cid: this_cid, bytes: this_bytes });
            }
            writer.close();
        } catch (e) {
            console.log(e);
        }
    }

    async convert_car_to_parquet(car_file, dst_path = null) {
        try {
            let car_path = car_file;
            const this_dir = path.dirname(import.meta.url).replace('file://', '');
            if (!car_path.startsWith('/')) {
                car_path = path.join(this_dir, car_path);
            }
            if (!dst_path) {
                dst_path = car_path.replace('.car', '.parquet');
            } else if (!dst_path.startsWith('/')) {
                dst_path = path.join(this_dir, dst_path);
            }
            const inStream = fs.createReadStream(car_path);
            const reader = await CarReader.fromIterable(inStream);
            const roots = await reader.getRoots();
            let records = [];
            for (let root of roots) {
                let got = await reader.get(root);
                const block = await Block.decode({
                    bytes: got.bytes,
                    codec: codec,
                    hasher: sha256,
                });
                let record = block.value;
                records.push(record);
            }
            // Infer schema from records
            let schemaFields = {};
            if (records.length > 0) {
                let sampleRecord = records[0];
                for (let key in sampleRecord) {
                    let value = sampleRecord[key];
                    let type;
                    if (typeof value === 'string') {
                        type = 'UTF8';
                    } else if (typeof value === 'number') {
                        type = Number.isInteger(value) ? 'INT64' : 'DOUBLE';
                    } else if (typeof value === 'boolean') {
                        type = 'BOOLEAN';
                    } else if (typeof value === 'bigint') {
                        // Parquet does not support BigInt directly, so we map it to INT64 or FIXED_LEN_BYTE_ARRAY
                        type = 'INT64';
                    } else {
                        // Handle nested objects or arrays if necessary
                        type = 'UTF8'; // Fallback for unsupported types
                    }
                    schemaFields[key] = { type: type };
                }
            }
            var schema = new parquetjs.ParquetSchema(schemaFields);
            var writer = await parquetjs.ParquetWriter.openFile(schema, dst_path);
            for (let record of records) {
                await writer.appendRow(record);
            }
            writer.close();
        } catch (e) {
            console.log(e);
        }
    }

    async test() {
        console.log('Hello from ipfs_parquet_to_car.js');
        let parquet_file =
            'bafkreidnskbqrb2uthybtvt7fazaxlzbdemci7acbozcfh2akerz6ujeza.parquet';
        let car_file =
            'bafkreidnskbqrb2uthybtvt7fazaxlzbdemci7acbozcfh2akerz6ujeza.car';

        try {
            await this.convert_parquet_to_car(parquet_file, 'example.car');
        } catch (e) {
            console.log(e);
        }

        try {
            await this.convert_car_to_parquet(car_file, 'example.parquet');
        } catch (e) {
            console.log(e);
        }

    }
}
export default ipfsParquetToCarJs;

const testIpfsParquetToCarJs = new ipfsParquetToCarJs();
testIpfsParquetToCarJs.test();
