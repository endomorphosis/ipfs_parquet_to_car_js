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
            const parquet_path = path.join(this_dir, parquet_file);
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


    async example() {
        const bytes = new TextEncoder().encode('random meaningless bytes')
        const hash = await sha256.digest(raw.encode(bytes))
        const cid = CID.create(1, raw.code, hash)
    
        // create the writer and set the header with a single root
        const { writer, out } = await CarWriter.create([cid])
        Readable.from(out).pipe(fs.createWriteStream('example.car'))
    
        // store a new block, creates a new file entry in the CAR archive
        await writer.put({ cid, bytes })

        const bytes2 = new TextEncoder().encode('more random meaningless bytes')
        const hash2 = await sha256.digest(raw.encode(bytes2))
        const cid2 = CID.create(1, raw.code, hash2)
        await writer.put({ cid: cid2, bytes: bytes2 })
        
        await writer.close()
    


        const inStream = fs.createReadStream('example.car')
        // read and parse the entire stream in one go, this will cache the contents of
        // the car in memory so is not suitable for large files.
        const reader = await CarReader.fromIterable(inStream)
    
        // read the list of roots from the header
        const roots = await reader.getRoots()
        // retrieve a block, as a { cid:CID, bytes:UInt8Array } pair from the archive
        const got = await reader.get(roots[0])
        // also possible: for await (const { cid, bytes } of CarIterator.fromIterable(inStream)) { ... }
    
        console.log(
        'Retrieved [%s] from example.car with CID [%s]',
        new TextDecoder().decode(got.bytes),
        roots[0].toString()
        )
    }
    


    async convert_to_parquet(){

    }

    async test(){
        console.log("Hello from ipfs_parquet_to_car.js");
        let parquet_file = "bafkreidnskbqrb2uthybtvt7fazaxlzbdemci7acbozcfh2akerz6ujeza.parquet";
        // try{
        //     await this.example()
        // }
        // catch(e){
        //     console.log(e);
        // }

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
