import data from '../resources/o2.csv'
import * as d3 from 'd3';


const parseResultWord = (word) => {
    // var value = word.replace(/\[|\]/gi, '').match(/(.+?)\((.+?)\)/g)
    var value = word.replace(/\[|\]/gi, '').match(/(.+?)\_(.+?)\((.+?)\)/g)
    return { text: RegExp.$2, value: RegExp.$3 }
}

const loadData = () => d3.csv(data, function(row) { 
    return { word: row.word, galls: row.galls.split(",") }
})

const getGallaries = (data) => {
    return data.map((d) => {
        const word = parseResultWord(d)
        return {id: word.text, value: word.value }
    })
}


export const getWordDict = async () => {
    const d = await loadData()
    const dict = {}
    d.map((row, index) => { 
        dict[row.word] = getGallaries(row.galls)
    })
    return dict
}


