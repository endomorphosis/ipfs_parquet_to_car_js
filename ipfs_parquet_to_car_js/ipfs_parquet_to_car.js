import parquetjs from '@dsnp/parquetjs';

export class ipfsParquetToCarJs{
    constructor(resources, metadata){
        this.resources = resources;
        this.metadata = metadata;
    }

    async convert_to_car(parquet_file){
        try{
            const parquet = await parquetjs.ParquetReader.openFile(parquet_file);
            const cursor = parquet.getCursor();
            let record = null;
            while (record = await cursor.next()) {
                console.log(record);
            }
        }catch(e){
            console.log(e);
        }

    }

    async convert_to_parquet(){

    }

    async test(){
        console.log("Hello from ipfs_parquet_to_car.js");
        parquet_file = "bafkreidnskbqrb2uthybtvt7fazaxlzbdemci7acbozcfh2akerz6ujeza.parquet";
        try{
            await this.convert_to_car(parquet_file);
        }
        catch(e){
            console.log(e);
        }

    }
}
export default ipfsParquetToCarJs;

const ipfsParquetToCarJs = new ipfsParquetToCarJs();
ipfsParquetToCarJs.test();
