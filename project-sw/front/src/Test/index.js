import React, { useState, useEffect, Fragment } from 'react';
import ReactWordcloud from 'react-wordcloud';
import * as d3 from 'd3';

// source data
import worddata from '../resources/sample.csv';

// css for react-wordcloud
import 'tippy.js/dist/tippy.css';
import 'tippy.js/animations/scale.css';


// 1. config for react-wordcloud
// wordcloud size
const size = [600, 400];

// options : https://react-wordcloud.netlify.app/props#options
const options = {
    rotations: 5,
    rotationAngles: [-45, 45],
    fontWeight: "bold",
    fontSizes: [30, 100]
};

// callbacks :  https://react-wordcloud.netlify.app/props#callbacks
const callbacks = {
    //getWordColor: word => word.value > 30 ? "blue" : "red", // function to set color of each word
    getWordTooltip: word => `${word.text} (${word.value})`, // function to set sentence to display on a tooltip when hovering over a word
    // onWordClick: console.log,
    // onWordMouseOver: console.log,
    // onWordMouseOut: console.log,
  }


// 2. data format convert function
const getData = () => d3.csv(worddata, function(row) { 
    return { words: row.words.split(","), galls: row.galls.split(",") }
})

const getWords = (data) => {
    return data.map((d) => {
        const sd = d.split("^")
        return { text: sd[0], value: sd[1] }
    })
}

const getTitle = (data) => {
    return data.map((d) => {
        const sd = d.split("^")
        return <li> {sd[0] + "(" + sd[1] + ")"} </li>
    })
}


// 3. main component & state
const initState = {
    galleries : [[]],
    words: [],
    index: 0
}

const TestResult = React.memo(() => {
    // set inital state
    const [state, setState] = useState(initState)

    // initial loading (when rendering)
    useEffect(() => getData().then((d) => {
        if (state.words.length == 0) {
            setState({...state, 
                galleries: (d.map((a) => getTitle(a.galls))),
                words: (d.map((a) => getWords(a.words)))
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
        <ul>{state.galleries[state.index]}</ul>
        <ReactWordcloud words={state.words[state.index]}
            callbacks={callbacks}
            options={options}
            size={size}
            />
    </Fragment>
})

export default TestResult;