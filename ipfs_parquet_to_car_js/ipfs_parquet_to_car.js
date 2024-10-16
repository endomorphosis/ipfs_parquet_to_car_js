import parquetjs from '@dsnp/parquetjs';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { Buffer } from 'buffer';
import { Readable } from 'stream'
import { CarReader, CarWriter } from '@ipld/car'
import * as raw from 'multiformats/codecs/raw'
import { CID } from 'multiformats/cid'
import { sha256 } from 'multiformats/hashes/sha2'


export class ipfsParquetToCarJs{
    constructor(resources, metadata){
        this.resources = resources;
        this.metadata = metadata;
        this.car_archive = null;
        this.parquet_archive = null;
        this.cids = [];
        this.bytes = [];
        this.hash = [];
    }

    async convert_to_car(parquet_file){
        try{
            const this_dir = path.dirname(import.meta.url).replace('file://', '');
            const parquet_path = path.join(this_dir, parquet_file);
            const parquet = await parquetjs.ParquetReader.openFile(parquet_path);
            const cursor = parquet.getCursor();
            let record = null;
            while (record = await cursor.next()) {
                // console.log(record);
                let json_record = JSON.stringify(record);
                let bytes = new TextEncoder().encode(json_record);
                console.log(bytes.toString());
                this.bytes.push(bytes);
                let hash = await sha256.digest(bytes);
                this.hash.push(hash);
                console.log(hash.toString());
                let cid = CID.create(1, raw.code, hash);
                this.cids.push(cid);
                console.log(cid.toString());
            }
            let all_bytes = Buffer.concat(this.bytes);
            let all_hash = await sha256.digest(all_bytes);
            let all_cid = CID.create(1, raw.code, all_hash);
            let { writer, out } = await CarWriter.create([all_cid])
            let output_file = path.join(this_dir, 'example.car');
            Readable.from(out).pipe(fs.createWriteStream(output_file))
            const len_cids = this.cids.length;
            for (let i = 0; i < len_cids; i++){
                let this_cid = this.cids[i];
                let this_bytes = this.bytes[i];
                await writer.put(this.cids[i], this.bytes[i]);
            }
            writer.close();
        }catch(e){
            console.log(e);
        }

    }

    async convert_to_parquet(){

    }

    async test(){
        console.log("Hello from ipfs_parquet_to_car.js");
        let parquet_file = "test-00000-of-00001.parquet";
        try{
            await this.convert_to_car(parquet_file);
        }
        catch(e){
            console.log(e);
        }

    }
}
export default ipfsParquetToCarJs;

const testIpfsParquetToCarJs = new ipfsParquetToCarJs();
testIpfsParquetToCarJs.test();
