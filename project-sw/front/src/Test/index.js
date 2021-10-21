import React, { useState, useEffect, Fragment } from 'react';
import ReactWordcloud from 'react-wordcloud';
import * as d3 from 'd3';

// source data
// import worddata from '../resources/sample.csv';
import worddata from '../resources/o1.csv';

// css for react-wordcloud
import 'tippy.js/dist/tippy.css';
import 'tippy.js/animations/scale.css';

import Grid from '@material-ui/core/Grid';
import Paper from '@material-ui/core/Paper';

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

const parseResultWord = (word) => {
    var value = word.replace(/\[|\]/gi, '').match(/(.+?)\((.+?)\)/g)
    return { text: RegExp.$1, value: RegExp.$2 }
}

// 2. data format convert function
const getData = () => d3.csv(worddata, function(row) { 
    return { words: row.words.split(","), galls: row.galls.split(",") }
})

const getWords = (data) => {
    return data.map((d) => {
        return parseResultWord(d)
    })
}

const getTitle = (data) => {
    return data.map((d) => {
        const word = parseResultWord(d)
        return <li> {word.text + "(" + word.value + ")"} </li>
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
    // return <Fragment>
    //     show next? <button onClick={increaseIndex}> click </button>
    //     <hr/>
    //     <h3>No. {state.index}</h3>
    //     <ul>{state.galleries[state.index]}</ul>
    //     <ReactWordcloud words={state.words[state.index]}
    //         callbacks={callbacks}
    //         options={options}
    //         size={size}
    //         />
    // </Fragment>

    const result = []
    for (let i = 0; i < state.words.length; i++) {
        result.push(
            <Fragment>
            <h3 key={"header"+i}>No. {i}</h3>
            <ul key={"content"+i}>{state.galleries[i]}</ul>
            <ReactWordcloud words={state.words[i]}
                callbacks={callbacks}
                options={options}
                size={size}
                />
            </Fragment>
        )
    }
    return <Grid container spacing={3}>
        {result.map((content, index) => {
            return <Grid item xs md lg key={"grid"+index}>
                    <Paper
                    sx={{
                        p: 2,
                        display: 'flex',
                        flexDirection: 'column',
                        height: 240,
                    }}
                    >
                    {content}
                    </Paper>
                </Grid>
        })}
    </Grid>
})

export default TestResult;