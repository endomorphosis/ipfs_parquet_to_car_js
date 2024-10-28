import { ipfsParquetToCarJs } from '../ipfs_parquet_to_car_js/ipfs_parquet_to_car.js';

export class test_parquet_to_car_js{
    constructor(){
        this.ipfsParquetToCarJs = new ipfsParquetToCarJs();
    }

    async init(){

    }

    async test(){
        let results = {};
        try{
            results["test"] = await this.ipfsParquetToCarJs.test();
        }
        catch(e){
            console.log(e);
        }
        return results;
    }
}


if (import.meta.url === 'file://' + process.argv[1]) {
    console.log("Running test");
    let test_results = {};
    try{
        const test = new test_parquet_to_car_js();
        test_results = await test.test();
    }
    catch(e){
        console.log(e);
    }
    console.log(test_results);
}