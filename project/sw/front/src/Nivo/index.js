// install (please make sure versions match peerDependencies)
// yarn add @nivo/core @nivo/circle-packing
import { ResponsiveCirclePacking } from '@nivo/circle-packing'

import React, { Fragment, useState, useEffect} from 'react';
import { MyResponsivePie } from '../NivoPie';
import CloudResult from '../WordGall';
import * as nivo from './convertToNivoData';

const idRegex = new RegExp("([0-9]+)\_(.*)");

const getOnlyId = (id) => {
    return idRegex.test(id) ? Number(id.replace(idRegex, "$1")) : Number(id)
}

// const MyResponsiveCirclePacking = ({ data /* see data tab */ }) => (
const MyResponsiveCirclePacking = React.memo(() => {
    const [zoomedId, setZoomedId] = useState(null)
    // TODO: 날짜 삽입
    const [data, setData] = useState({id: "20211022", children: []})
    const [words, setWords] = useState([])
    const [node, setNode] = useState(null)

    // const idRegex = new RegExp("([0-9]+)\_(.*)");

    useEffect(() => nivo.loadData(data.id).then((d) => {
        setData({id: data.id, children: nivo.getNivoData(d) })
        setWords((d.map((a) => nivo.getWords(a.words))))
    }), [])

    const getWord = (index) => {
        if (!isNaN(Number(index)) && index <= words.length) {
            return words[index-1]
        }
        return null
    }
    const getNode = (index) => {
        if (!isNaN(Number(index)) && index <= data.children.length) {
            return data.children[index-1]
        }
        return null
    }

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
