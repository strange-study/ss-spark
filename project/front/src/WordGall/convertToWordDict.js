import * as d3 from 'd3';

const parseResultWord = (word) => {
    // var value = word.replace(/\[|\]/gi, '').match(/(.+?)\_(.+?)\((.+?)\)/g)
    // return { text: RegExp.$2, value: RegExp.$3 }
    const value = word.replace(/\(|\)/gi, '')
    return { id: value.split(",")[0], value: Number(value.split(",")[1]) };
}

const loadData = () => d3.csv(`${process.env.PUBLIC_URL}/resources/minsw_1.csv`, function(row) { 
    return { word: row.word, galls: row.galls.match(/\((.+?),(.+?)\)/g) }
})

const getGallaries = (data) => {
    return data.map((d) => parseResultWord(d))
}

export const getWordDict = async () => {
    const d = await loadData()
    const dict = {}
    d.map((row, index) => { 
        dict[row.word] = getGallaries(row.galls)
    })
    return dict
}


