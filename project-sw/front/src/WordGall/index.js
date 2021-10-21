import React, { useState, useEffect, Fragment } from 'react';
import ReactWordcloud from 'react-wordcloud';
import * as d3 from 'd3';

// source data
// import worddata from '../resources/sample.csv';
import worddata from '../resources/o1.csv';

// css for react-wordcloud
import 'tippy.js/dist/tippy.css';
import 'tippy.js/animations/scale.css';

import { getWordDict } from '../WordGall/convertToWordDict';


import Avatar from '@material-ui/core/Avatar';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemAvatar from '@material-ui/core/ListItemAvatar';
import ListItemText from '@material-ui/core/ListItemText';
import DialogTitle from '@material-ui/core/DialogTitle';
import Dialog from '@material-ui/core/Dialog';
import ExitToAppIcon from '@material-ui/icons/ExitToApp';
import { blue } from '@material-ui/core/colors';


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

const GallDialog = ({ onClose, data, open }) => {
  
    return (
      <Dialog onClose={onClose} open={open}>
        <DialogTitle>Top Galls of '{data.word}'</DialogTitle>
        <List sx={{ pt: 0 }}>
          {data.galls.map((d) => (
            <ListItem button onClick={e => window.open("https://gall.dcinside.com/board/lists?id=" + d.id)} key={d.id}>
              <ListItemAvatar>
                <Avatar sx={{ bgcolor: blue[100], color: blue[600] }}>
                  <ExitToAppIcon />
                </Avatar>
              </ListItemAvatar>
              <ListItemText primary={`${d.id} (${d.value})`} />
            </ListItem>
          ))}
        </List>
      </Dialog>
    );
  }
  

const CloudResult = (props) => {
    const [state, setState] = useState(null)
    const [top, setTop] = useState({word: null, galls: []})
    const [open, setOpen] = useState(false)
  
    useEffect(() => getWordDict().then((d) => {
        setState(d)
    }), [])

    const callbacks = {
        getWordTooltip: word => `${word.text} (${word.value})`,
        onWordClick: word => {
            setTop({word: word.text, galls: state[word.text]})
            handleClickOpen()
        }
    }

    const handleClickOpen = () => {
        setOpen(true);
    };

    const handleClose = (value) => {
        setOpen(false);
    };
    
    if (!props.data) {
        return <div/>
    }
    return <Fragment>
        <ReactWordcloud words={props.data}
            callbacks={callbacks}
            options={options}
            size={size}
            />
        <GallDialog
            data={top}
            open={open}
            onClose={handleClose}
        />
    </Fragment>
}

export default CloudResult;