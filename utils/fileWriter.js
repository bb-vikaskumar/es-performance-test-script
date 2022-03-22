const fs = require('fs');



class FileWriter {
    static write({file_name='output.txt', data}) {
        const filePath = `./dumps/${file_name}`;
        
        fs.writeFile(filePath, data, (err) => {
            if (err) throw err;

            console.log('[sucess] successfully dumped ES data to ', filePath);
        })
    }

    static append({file_name='output.txt', data, endCharater=''}) {
        const filePath = `./dumps/${file_name}`;
        
        fs.appendFile(filePath, data + endCharater, (err) => {
            if (err) throw err;

            console.log('[sucess] successfully dumped ES data to ', filePath);
        })
    }
}

module.exports = FileWriter;
