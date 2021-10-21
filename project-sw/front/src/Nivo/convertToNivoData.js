
import data from '../resources/o1.csv'
import * as d3 from 'd3';


const parseResultWord = (word) => {
    // var value = word.replace(/\[|\]/gi, '').match(/(.+?)\((.+?)\)/g)
    var value = word.replace(/\[|\]/gi, '').match(/(.+?)\_(.+?)\((.+?)\)/g)
    return { text: RegExp.$2, value: RegExp.$3 }
}

export const loadData = () => d3.csv(data, function(row) { 
    return { words: row.words.split(","), galls: row.galls.split(",") }
})

const getWords = (data) => {
    return data.map((d) => {
        return parseResultWord(d)
    })
}

const getGallaries = (data, prefix) => {
    // return data.map((d) => {
    //     const word = parseResultWord(d)
    //     return <li> {word.text + "(" + word.value + ")"} </li>
    // })
    return data.map((d) => {
        const word = parseResultWord(d)
        return {id: prefix + "_" + word.text, value: (40-prefix)  * word.value }
    })
}


const getNivoData = async () => {
    const d = await loadData()
    // return { galleries: (d.map((a) => getTitle(a.galls))), words: (d.map((a) => getWords(a.words))) }
    const group = (d.map((a, index) => { return { galls : getGallaries(a.galls, index), words: getWords(a.words)}}))
    return group.map((concept, index) => { return { id: index, children: concept.galls } })
}



// {
//     // must be unique for the whole dataset
//     id: string | number
//     value: number
//     children: {
//         id: string | number
//         value: number
//         children: ...
//     }[]
// }