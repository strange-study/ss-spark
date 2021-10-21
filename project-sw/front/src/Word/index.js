// import React, { useState, useEffect, Fragment } from 'react';
// import ReactWordcloud from 'react-wordcloud';
// import * as d3 from 'd3';

// // source data
// // import worddata from '../resources/sample.csv';
// import worddata from '../resources/o1.csv';

// // css for react-wordcloud
// import 'tippy.js/dist/tippy.css';
// import 'tippy.js/animations/scale.css';

// import Grid from '@material-ui/core/Grid';
// import Paper from '@material-ui/core/Paper';

// // 1. config for react-wordcloud
// // wordcloud size
// const size = [600, 400];

// // options : https://react-wordcloud.netlify.app/props#options
// const options = {
//     rotations: 5,
//     rotationAngles: [-45, 45],
//     fontWeight: "bold",
//     fontSizes: [30, 100]
// };

// // callbacks :  https://react-wordcloud.netlify.app/props#callbacks
// const callbacks = {
//     //getWordColor: word => word.value > 30 ? "blue" : "red", // function to set color of each word
//     getWordTooltip: word => `${word.text} (${word.value})`, // function to set sentence to display on a tooltip when hovering over a word
//     // onWordClick: console.log,
//     // onWordMouseOver: console.log,
//     // onWordMouseOut: console.log,
//   }


// const CloudResult = (props) => {
//     // // set inital state
//     // const [state, setState] = useState(initState)

//     // // initial loading (when rendering)
//     // useEffect(() => getData().then((d) => {
//     //     if (state.words.length == 0) {
//     //         setState({...state, 
//     //             galleries: (d.map((a) => getTitle(a.galls))),
//     //             words: (d.map((a) => getWords(a.words)))
//     //         })
//     //     }
//     // }))

//     // const increaseIndex = () => {
//     //     const nextIndex = (state.index < state.words.length - 1) ? state.index + 1 : 0
//     //     setState({...state, index: nextIndex})
//     // }
//     console.log("me!", props)
//     if (!props.data) {
//         return <div/>
//     }
//     return <Fragment>
//         <ReactWordcloud words={props.data}
//             callbacks={callbacks}
//             options={options}
//             size={size}
//             />
//     </Fragment>
// }

// export default CloudResult;