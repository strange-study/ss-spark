import React, {Fragment, useEffect, useState} from 'react';
import ReactWordcloud from 'react-wordcloud';
import * as d3 from 'd3';


import 'tippy.js/dist/tippy.css';
import 'tippy.js/animations/scale.css';


let date = new Date()
let currentDate = `${date.year}${date.month >= 10 ? date.month : '0' + date.month}${date.date >= 10 ? date.date : '0' + date.date}`


const size = [1200, 800];

const callbacks = {
    getWordTooltip: word => `${word.text} (${word.value})`, // function to set sentence to display on a tooltip when hovering over a word
}

const options = {
    rotations: 5,
    rotationAngles: [-45, 45],
    fontWeight: "bold",
    fontSizes: [1, 50]
};

const getData = () => d3.csv(`${process.env.PUBLIC_URL}/resources/mk_1.csv`, function (row) {
    return {DATE: row.DATE, SCORE_WORDS: row.SCORE_WORDS.split(",")}
})

const getWords = (inputData) => {
    return inputData.map((data) => {
        const splitData = data.split(" -> ")
        return {text: splitData[0], value: (splitData[1])}
    })
}

const getTitle = (data) => {
    return <li> {data} </li>
}


// 3. main component & state
const initState = {
    dates: [],
    words: [],
    index: 0
}

const TestResult = React.memo(() => {
    // set inital state
    const [state, setState] = useState(initState)

    // initial loading (when rendering)
    useEffect(() => getData().then((d) => {
        if (state.words.length === 0) {
            setState({
                ...state,
                dates: (d.map((a) => getTitle(a.DATE))),
                words: (d.map((a) => getWords(a.SCORE_WORDS)))
            })
        }
    }))

    const increaseIndex = () => {
        const nextIndex = (state.index < state.words.length - 1) ? state.index + 1 : 0
        setState({...state, index: nextIndex})
    }

    // Layout
    //  - <Fragment> => grouping some components
    //  - <ReactWordcloud> => react-wordcloud component (ref. https://react-wordcloud.netlify.app/)
    return <Fragment>
        show next? <button onClick={increaseIndex}> click </button>
        <hr/>
        <h3>No. {state.index}</h3>
        <ul>{state.dates[state.index]}</ul>
        <ReactWordcloud words={state.words[state.index]}
                        callbacks={callbacks}
                        options={options}
                        size={size}
        />
    </Fragment>
})

export default TestResult;