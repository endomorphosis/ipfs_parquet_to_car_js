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

    async convert_parquet_to_car(parquet_file, dst_path=null){
        try{
            const this_dir = path.dirname(import.meta.url).replace('file://', '');
            let parquet_path = parquet_file;
            if (!parquet_path.startsWith('/')){
                parquet_path = path.join(this_dir, parquet_path);
            }
            if (!dst_path){
                dst_path = parquet_path.replace('.parquet', '.car');
            }
            else if (!dst_path.startsWith('/')){
                dst_path = path.join(this_dir, dst_path);
            }
            const parquet = await parquetjs.ParquetReader.openFile(parquet_path);
            const cursor = parquet.getCursor();
            let record = null;
            while (record = await cursor.next()) {
                let json_record = JSON.stringify(record, (key, value) => 
                    typeof value === 'bigint' ? value.toString() : value
                );
                let bytes = new TextEncoder().encode(json_record);
                this.bytes.push(bytes);
                let hash = await sha256.digest(raw.encode(bytes));
                this.hash.push(hash);
                this.cids.push(CID.create(1, raw.code, hash));
            }
            let { writer, out } = await CarWriter.create([this.cids[0]]);
            let output_file = path.join(this_dir, 'example.car');
            if (dst_path){
                Readable.from(out).pipe(fs.createWriteStream(parquet_path.replace('.parquet', '.car')));
            }
            else{
                Readable.from(out).pipe(fs.createWriteStream(parquet_path.replace('.parquet', '.car')));
            }
            const len_cids = this.cids.length;
            for (let i = 0; i < len_cids; i++){
                let this_cid = this.cids[i];
                let this_bytes = this.bytes[i];
                let this_hash = this.hash[i];
                await writer.put({cid: this_cid, bytes: this_bytes });
            }
            writer.close();
        }catch(e){
            console.log(e);
        }
    }

    async convert_car_to_parquet(car_file, dst_path=null){
        try{
            let car_path = car_file;
            const this_dir = path.dirname(import.meta.url).replace('file://', '');
            if (!car_path.startsWith('/')){
                car_path = path.join(this_dir, car_path);
            }
            if (!dst_path){
                dst_path = car_path.replace('.car', '.parquet');
            }
            else if (!dst_path.startsWith('/')){
                dst_path = path.join(this_dir, dst_path);
            }
            const inStream = fs.createReadStream(car_path);
            const reader = await CarReader.fromIterable(inStream);
            const roots = await reader.getRoots();
            const len_roots = roots.length;
            var schema = new parquetjs.ParquetSchema({
                items: {
                    type: 'UTF8',
                }
            });
            var writer = await parquetjs.ParquetWriter.openFile(schema, 'fruits.parquet');
            for (let i = 0; i < len_roots; i++){
                let root = roots[i];
                let got = await reader.get(root);
                let json_record = new TextDecoder().decode(got.bytes);
                let record = JSON.parse(json_record);
                await writer.appendRow({items: json_record});
            }
            writer.close();
        }catch(e){
            console.log(e);
        }
    }

    async test(){
        console.log("Hello from ipfs_parquet_to_car.js");
        let parquet_file = "bafkreidnskbqrb2uthybtvt7fazaxlzbdemci7acbozcfh2akerz6ujeza.parquet";
        let car_file = "bafkreidnskbqrb2uthybtvt7fazaxlzbdemci7acbozcfh2akerz6ujeza.car";
        try{
            await this.convert_car_to_parquet(car_file, 'example.parquet');
        }
        catch(e){
            console.log(e);
        }

        try{
            await this.convert_parquet_to_car(parquet_file, 'example.car');
        }
        catch(e){
            console.log(e);
        }

    }
}
export default ipfsParquetToCarJs;

const testIpfsParquetToCarJs = new ipfsParquetToCarJs();
testIpfsParquetToCarJs.test();
