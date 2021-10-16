import React, { useState, useEffect, useMemo } from 'react';
import ReactWordcloud from 'react-wordcloud';
import { Container, Row, Col, Dropdown, DropdownButton } from 'react-bootstrap';
import * as d3 from 'd3';
import data from './resources/data.csv';

import 'tippy.js/dist/tippy.css';
import 'tippy.js/animations/scale.css';
import 'bootstrap/dist/css/bootstrap.min.css';
import './DcWordCloud.css'

const initWords = [
  {
    text: 'told',
    value: 64,
  },
  {
    text: 'mistake',
    value: 11,
  },
  {
    text: 'thought',
    value: 16,
  },
  {
    text: 'bad',
    value: 17,
  },
]


const getColorCode = function(word) {
  let hash = 0, i, chr;
  if (word.length === 0) return hash;
  for (i = 0; i < word.length; i++) {
    chr   = word.charCodeAt(i);
    hash  = ((hash << 5) - hash) + chr;
    hash |= 0; // Convert to 32bit integer
  }
  return `#${hash.toString(16)}`;
}

const GallerySelector = ({galleries, setSelectedGallery}) => {
  const [selected, setSelected] = useState('Choose Gallery')

  const handleSelect = (eventKey) => {
    setSelectedGallery(eventKey);
    setSelected(eventKey);
  }

  return ( <>
      <DropdownButton id="gallery-selector" title={selected} onSelect={handleSelect}>
        {galleries._names && [ ...galleries._names ].map((gallery) => <Dropdown.Item eventKey={gallery}>{gallery}</Dropdown.Item>)}
      </DropdownButton>
    </>
  );
}

const callbacks = {
  getWordColor: word => getColorCode(word.text),
  onWordClick: console.log,
  onWordMouseOver: console.log,
  getWordTooltip: word => `${word.text} (hashCode: ${getColorCode(word.text)})`,
}

const DcWordCloud = () => {
  const [galleries, setGalleries] = useState([]);
  const [selectedGallery, setSelectedGallery] = useState('');
  const [words, setWords] = useState(initWords);

  useEffect(() => {
    d3.csv(data).then((rows) => {
      const galleries = {};
      const _names = new Set();

      rows.reduce((result, row) => {
        _names.add(row.gallery);
        galleries[row.gallery]=galleries[row.gallery] ? {...galleries[row.gallery]} : {};
        galleries[row.gallery][row.date] = row.termFreqs.match(/\((.+?),(.+?)\)/g).map((value) => {
          value = value.replace(/\(|\)/gi, '')
          const term = value.split(",")[0]
          const freq = Number(value.split(",")[1])
  
          return {text: term, value: freq};
        });
      }, {});

      galleries['_names'] = _names;
      setGalleries(galleries);
    });
  }, []);

  const drawWordCloud = () => {
    const dates = selectedGallery && Object.getOwnPropertyNames(galleries[selectedGallery]);
    return (dates && dates.map((date) => (
      <Col><div style={{ textAlign: 'center' }}><ReactWordcloud callbacks={callbacks} words={galleries[selectedGallery][date]} />{`<${date}>`}</div></Col>
      )))
  };
  
  return ( <>
    <GallerySelector galleries={galleries} setSelectedGallery={setSelectedGallery}/>
    <Container>
      <Row>
        {useMemo(() => drawWordCloud(), [selectedGallery])}
      </Row>
    </Container>
  </> ); 
}

export default DcWordCloud