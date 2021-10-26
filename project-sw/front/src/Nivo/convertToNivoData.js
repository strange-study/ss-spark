// TODO: 날짜에 따른 리소스 load
import data from '../resources/minsw_1.csv';
import * as d3 from 'd3';


const parseResultWord = (word) => {
    // var value = word.replace(/\[|\]/gi, '').match(/(.+?)\_(.+?)\((.+?)\)/g)
    // return { text: RegExp.$2, value: RegExp.$3 }
    const value = word.replace(/\(|\)/gi, '')
    return { text: value.split(",")[0], value: Number(value.split(",")[1]) };
}

const loadData = (id) => {
    return d3.csv(data, function(row) { 
        return { words: row.words.match(/\((.+?),(.+?)\)/g), galls: row.galls.match(/\((.+?),(.+?)\)/g) }
    })
}

const getWords = (data) => {
    return data.map((d) => {
        return parseResultWord(d)
    })
}

const getGallaries = (data, prefix) => {
    return data.map((d) => {
        const word = parseResultWord(d)
        return { id: prefix + "_" + word.text, value: (40-prefix)  * word.value, name: word.text, weight: word.value }
    })
}

const getNivoData =  (data) => {
    // return { galleries: (d.map((a) => getTitle(a.galls))), words: (d.map((a) => getWords(a.words))) }
    const group = (data.map((a, index) => { return { galls : getGallaries(a.galls, index+1), words: getWords(a.words)}}))
    return group.map((concept, index) => { return { id: index+1, children: concept.galls } })
}

export {
    loadData,
    getWords,
    getGallaries,
    getNivoData
}