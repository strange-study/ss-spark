// install (please make sure versions match peerDependencies)
// yarn add @nivo/core @nivo/circle-packing
import { ResponsiveCirclePacking } from '@nivo/circle-packing'

import React, { Fragment, useState, useEffect} from 'react';
import { MyResponsivePie } from '../NivoPie';
import CloudResult from '../WordGall';
import { loadData } from './convertToNivoData';

// make sure parent container have a defined height when using
// responsive component, otherwise height will be 0 and
// no chart will be rendered.
// website examples showcase many properties,
// you'll often use just a few of them.

// import data from '../resources/nivo-test.json';

const parseNameResultWord = (word) => {
    // var value = word.replace(/\[|\]/gi, '').match(/(.+?)\((.+?)\)/g)
    var value = word.replace(/\[|\]/gi, '').match(/(.+?)\_(.+?)\((.+?)\)/g)
    return { text: RegExp.$2, value: RegExp.$3 }
}
const parseWordResultWord = (word) => {
    var value = word.replace(/\[|\]/gi, '').match(/(.+?)\((.+?)\)/g)
    return { text: RegExp.$1, value: RegExp.$2 }
}

const getGallaries = (data, prefix) => {
    // return data.map((d) => {
    //     const word = parseResultWord(d)
    //     return <li> {word.text + "(" + word.value + ")"} </li>
    // })
    return data.map((d) => {
        const word = parseNameResultWord(d)
        return { id: prefix + "_" + word.text, value: (40-prefix)  * word.value, name: word.text, weight: word.value }
    })
}

const getNivoData =  (data) => {
    // return { galleries: (d.map((a) => getTitle(a.galls))), words: (d.map((a) => getWords(a.words))) }
    const group = (data.map((a, index) => { return { galls : getGallaries(a.galls, index), words: getWords(a.words)}}))
    return group.map((concept, index) => { return { id: index, children: concept.galls } })
}

const getWords = (data) => {
    return data.map((d) => {
        return parseWordResultWord(d)
    })
}

const idRegex = new RegExp("([0-9]+)\_(.*)");

const getOnlyId = (id) => {
    return idRegex.test(id) ? Number(id.replace(idRegex, "$1")) : Number(id)
}

// const MyResponsiveCirclePacking = ({ data /* see data tab */ }) => (
const MyResponsiveCirclePacking = React.memo(() => {
    const [zoomedId, setZoomedId] = useState(null)
    const [data, setData] = useState({id: "20210818", children: []})
    const [words, setWords] = useState([])
    const [node, setNode] = useState(null)

    // const idRegex = new RegExp("([0-9]+)\_(.*)");

    useEffect(() => loadData().then((d) => {
        setData({id: data.id, children: getNivoData(d) })
        setWords((d.map((a) => getWords(a.words))))
    }), [])

    const getWord = (index) => {
        if (!isNaN(Number(index)) && index < words.length) {
            console.log(words[index])
            return words[index]
        }
        return null
    }
    const getNode = (index) => {
        if (!isNaN(Number(index)) && index < data.children.length) {
            return data.children[index]
        }
        return null
    }

    console.log(words)

    return <Fragment>
        <div style={{height:600}}>
            <ResponsiveCirclePacking
                data={data}
                margin={{ top: 20, right: 20, bottom: 20, left: 20 }}
                // id="name"
                // value="loc"
                colors={{ scheme: 'nivo' }}
                childColor="black"
                padding={4}
                enableLabels={true}
                label={function(e){
                    return idRegex.test(e.id) ? e.id.replace(idRegex, "$2") : e.id
                }}
                labelsFilter={function(e){return 2===e.node.depth}}
                labelsSkipRadius={20}
                labelTextColor={{ from: 'color', modifiers: [ [ 'darker', 2 ] ] }}
                borderWidth={1}
                borderColor={{ from: 'color', modifiers: [ [ 'darker', 0.5 ] ] }}
                defs={[
                    {
                        id: 'lines',
                        type: 'patternLines',
                        background: 'none',
                        color: 'inherit',
                        rotation: -45,
                        lineWidth: 5,
                        spacing: 8
                    }
                ]}
                fill={[ { match: { depth: 1 }, id: 'lines' } ]}
                zoomedId={zoomedId}
                onClick={node => {
                    setZoomedId(zoomedId === getOnlyId(node.id) ? null : getOnlyId(node.id)) // not integer (leaf 면 밑에 띄우기)
                }}
                tooltip={({ id, value, color }) => <strong style={{ color }}> {getOnlyId(id)} </strong>}
                theme={{
                    tooltip: {
                        container: {
                            background: '#333',
                        },
                    },
                }}
            />
        </div>
        <br/>
        zoom : {zoomedId}
        <div style={{height:300}}>
        <MyResponsivePie data={getNode(zoomedId)}/>
        </div>
        <CloudResult data={getWord(zoomedId)}/>


    </Fragment>
})

export default MyResponsiveCirclePacking;
