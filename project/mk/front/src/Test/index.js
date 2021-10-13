import React, {Fragment, useEffect, useState} from 'react';
import ReactWordcloud from 'react-wordcloud';
import * as d3 from 'd3';

import worddata from '../resources/bitcoin_result.csv';

import 'tippy.js/dist/tippy.css';
import 'tippy.js/animations/scale.css';


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


const getWords = (inputData) => {
    return inputData && inputData.map((data) => {
        const splitData = data.split(' -> ')
        return {text: splitData[0], value: (splitData[1])}
    })
}

const getTitle = (data) => {
    return <li> {data} </li>
}

// 3. main component & state
const initState = {
    dates: [],
    words: []
}

const TestResult = React.memo(() => {
    // set inital state
    const [index, setIndex] = useState(0);
    const [dates, setDates] = useState([]);
    const [wordsAndScore, setWordsAndScore] = useState([]);

    useEffect(() => {
        d3.csv(worddata).then(function (data) {
            const scoreMap = new Map()
            const dates = [];
            let wordsAndScore = [];

            let conceptWeight = data.length + 1
            for (let i = 0; i < data.length; i++) {
                data[i].BEST_WORDS.match(/\((.+?),(.+?)\)/g).forEach(function (word) {
                    const wordAndScore = word.replace(/\(|\)/gi, '').split(",")
                    data[i].GALL_ID.match(/\((.+?),(.+?)\)/g).forEach(function (id) {
                            const idAndScore = id.replace(/\(|\)/gi, '').split(",")
                            let wordScore = wordAndScore[1];
                            let idScore = idAndScore[1]

                            if (wordScore < 0) {
                                wordScore = 0
                            } else {
                                wordScore = wordScore * 2
                            }
                            if (idScore < 0) {
                                idScore = 0
                            } else {
                                idScore = idScore * 2
                            }
                            if (!scoreMap.has(idAndScore[0])) {
                                const wordMap = new Map()
                                wordMap.set(wordAndScore[0], 0)
                                scoreMap.set(idAndScore[0], wordMap)
                            }
                            let wordMap = scoreMap.get(idAndScore[0])
                            if (!wordMap.has(wordAndScore[0])) {
                                wordMap.set(wordAndScore[0], 0)
                            }
                            wordMap.set(wordAndScore[0], wordMap.get(wordAndScore[0]) + (Math.pow(wordScore * idScore, 2) * conceptWeight))
                            scoreMap.set(idAndScore[0], wordMap)

                        }
                    )

                })
                conceptWeight -= 1
            }

            let tempDates = Array.from(scoreMap.keys());
            tempDates.forEach(function (date) {
                dates.push(date)
            })
            let values = Array.from(scoreMap.values());
            values.forEach(function (scoreMap) {
                let scoreMapKeys = Array.from(scoreMap.keys());
                let scoreMapValues = Array.from(scoreMap.values());
                let tempResult = []
                for (let i = 0; i < scoreMapKeys.length; i++) {
                    tempResult.push(`${scoreMapKeys[i]} -> ${scoreMapValues[i]}`)
                }
                wordsAndScore.push(tempResult)
            })

            setDates(dates);
            setWordsAndScore(wordsAndScore);
        })
    }, []);

    const increaseIndex = () => {
        const nextIndex = (index < wordsAndScore.length - 1) ? index + 1 : 0 ;
        setIndex(nextIndex);
    }

    // Layout
    //  - <Fragment> => grouping some components
    //  - <ReactWordcloud> => react-wordcloud component (ref. https://react-wordcloud.netlify.app/)
    return <Fragment>
        show next? <button onClick={increaseIndex}> click </button>
        <hr/>
        <h3>No. {index}</h3>
        <ul>{getTitle(dates[index])}</ul>
        <ReactWordcloud words={getWords(wordsAndScore[index])}
                        callbacks={callbacks}
                        options={options}
                        size={size}
        />
    </Fragment>
})

export default TestResult;