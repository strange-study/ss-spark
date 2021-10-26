import React, { Fragment } from 'react';
import PropTypes from 'prop-types';
import { makeStyles } from '@material-ui/core/styles';
import Paper from '@material-ui/core/Paper';
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Typography from '@material-ui/core/Typography';
import Box from '@material-ui/core/Box';
import Link from '@material-ui/core/Link';


import Card from '@material-ui/core/Card';
import CardActions from '@material-ui/core/CardActions';
import CardContent from '@material-ui/core/CardContent';

import Button from '@material-ui/core/Button';
import Grid from '@material-ui/core/Grid';
import Divider from '@material-ui/core/Divider';
import Container from '@material-ui/core/Container';

import MyResponsiveCirclePacking from './Nivo';
import TestResult from './Test';
import DcWordCloud from './DcWordCloud';

function TabPanel(props) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box p={3}>
          <Typography>{children}</Typography>
        </Box>
      )}
    </div>
  );
}

TabPanel.propTypes = {
  children: PropTypes.node,
  index: PropTypes.any.isRequired,
  value: PropTypes.any.isRequired,
};

function a11yProps(index) {
  return {
    id: `simple-tab-${index}`,
    'aria-controls': `simple-tabpanel-${index}`,
  };
}

const useStyles = makeStyles((theme) => ({
  root: {
    ...theme.typography.button,
    flexGrow: 1,
    backgroundColor: theme.palette.background.paper,
  },
  title: {
    margin: theme.spacing(10, 10),
  },
  section1: {
    margin: theme.spacing(3, 2),
    width: 500
  },
}));


const Copyright = (props) => {
  return (
    <Typography variant="body2" color="primary" align="center" {...props}>
      {'Copyright © - '}
      <Link color="inherit" href="https://github.com/strange-study/ss-spark">
        @stransge-study/ss-spark
      </Link>{' '}
      {new Date().getFullYear()}
      {'.'}
    </Typography>
  );
}


const MiddleDividers = (props) => {
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <Card className={classes.section1}>
        <CardContent>
        <Grid container alignItems="center">
          <Grid item xs>
            <Typography gutterBottom variant="h4">
              {props.title}
            </Typography>
          </Grid>
          <Grid item>
            <Typography gutterBottom variant="h6">
              {props.name}
            </Typography>
          </Grid>
        </Grid>
        <Typography color="textSecondary" variant="body2">
          {props.content}
        </Typography>
        </CardContent>
        <CardActions>
        <Button color="primary" href={props.link}> → Go to Github</Button>
        </CardActions>
      </Card>
      <Divider variant="middle" />
    </div>
  );
}

export default function SimpleTabs() {
  const classes = useStyles();
  const [value, setValue] = React.useState(0);

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };

  return (
    <Container maxWidth="xl">
      <Grid container justifyContent="center" alignItems="center">
        <Grid item className={classes.title}>
          <Typography gutterBottom variant="h3">SS-SPARK</Typography>
        </Grid>
      </Grid>
      <Paper className={classes.root}>
        <Tabs
        value={value}
        onChange={handleChange}
        indicatorColor="primary"
        textColor="primary"
        centered
        >
          <Tab label="Item 1 (SW)" {...a11yProps(0)} />
          <Tab label="Item 2 (MK)" {...a11yProps(1)} />
          <Tab label="Item 3 (JJ)" {...a11yProps(2)} />
        </Tabs>
        <TabPanel value={value} index={0}>
          <MiddleDividers 
            title="ITEM 1" 
            name="@minSW" 
            content="TODAY HOT CONCEPTS, KEYWORDS IN DC-INSIDE TOP 100 COMMUNITY"
            link="https://github.com/strange-study/ss-spark/tree/project/%2350"
          />
          <MyResponsiveCirclePacking/>
        </TabPanel>
        <TabPanel value={value} index={1}>
          <MiddleDividers 
            title="ITEM 2" 
            name="@ChoMk" 
            content="THIS WEEK HOT KEYWORDS IN DC-INSIDE 'BITCOINS' COMMUNITY"
            link="https://github.com/strange-study/ss-spark/tree/feature/mk_dc"
          />
          <TestResult/>
        </TabPanel>
        <TabPanel value={value} index={2}>
          <MiddleDividers 
            title="ITEM 3" 
            name="@jin5335" 
            content="THIS WEEK HOT CONCEPTS, KEYWORDS IN DC-INSIDE TOP 100 COMMUNITY"
            link="https://github.com/strange-study/ss-spark/tree/project/%2353"
          />
          <DcWordCloud/>
        </TabPanel>
      </Paper>
      <Copyright sx={{ pt: 4 }} />
    </Container>
  );
}


// import React from 'react';
// import { makeStyles } from '@material-ui/core/styles';
// import Paper from '@material-ui/core/Paper';
// import Tabs from '@material-ui/core/Tabs';
// import Tab from '@material-ui/core/Tab';

// const useStyles = makeStyles({
//   root: {
//     flexGrow: 1,
//   },
// });

// export default function CenteredTabs() {
//   const classes = useStyles();
//   const [value, setValue] = React.useState(0);

//   const handleChange = (event, newValue) => {
//     setValue(newValue);
//   };

//   return (
//     <Paper className={classes.root}>
//       <Tabs
//         value={value}
//         onChange={handleChange}
//         indicatorColor="primary"
//         textColor="primary"
//         centered
//       >
//         <Tab label="Item One" />
//         <Tab label="Item Two" />
//         <Tab label="Item Three" />
//       </Tabs>
//     </Paper>
//   );
// }
