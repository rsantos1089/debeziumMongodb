CREATE  FUNCTION analytics_video.json2array(json STRING)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  if (json == null) {
  return null;
  } 
  else {
  return JSON.parse(json).map(x=>JSON.stringify(x));
  }
"""; 