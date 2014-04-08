function include(filename)
{
    var head = document.getElementsByTagName('head')[0];
   
    script = document.createElement('script');
    script.src = filename;
    script.type = 'text/javascript';
   
    head.appendChild(script)
}

include("http://d3js.org/d3.v3.min.js");

function drawTimeseries(dataset, nb){
	drawChartTimeseries(parseDataTimeseries(dataset));
}

function parseDataTimeseries(dataset){
	var keys = d3.keys(dataset);
	var data = keys.map(function(name) {
		return {
			name:name,
			values: dataset[name].data.map(function(c) {
				return {
					date: new Date(c[0]), 
					sensor_value: +c[1]
				};
			})
		};		  
	});
	return data;
}

function drawChartTimeseries(data){
	
	var color = d3.scale.category20();
	
	var margin = {top: 40, right: 60, bottom: 500, left: 80},
		width = 700 - margin.left - margin.right,
		height = 800 - margin.top - margin.bottom;
	
	var x = d3.time.scale()
		.range([0, width])

	var y = d3.scale.linear()
		.range([height, 0]);

	var xAxis = d3.svg.axis()
		.scale(x)
		.orient("bottom")
		.tickFormat(d3.time.format.utc("%Y-%m-%d %H:%M:%S"));

	var yAxis = d3.svg.axis()
		.scale(y)
		.orient("left");
		
	var line = d3.svg.line()
		.interpolate("basis")
		.x(function(d) { return x(d.date); })
		.y(function(d) { return y(d.sensor_value); });
		
	var svg = d3.select("#plotContainer").append("svg")
		.attr("width", width + margin.left + margin.right)
		.attr("height", height + margin.top + margin.bottom)
		.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")");

	
	
	x.domain([
		d3.min(data, function(c) { return d3.min(c.values, function(v) { return v.date; }); }),
		d3.max(data, function(c) { return d3.max(c.values, function(v) { return v.date; }); })
	  ]);

    y.domain([
		d3.min(data, function(c) { return d3.min(c.values, function(v) { return v.sensor_value; }); }),
		d3.max(data, function(c) { return d3.max(c.values, function(v) { return v.sensor_value; }); })
	  ]);
		
	svg.append("g")
		.attr("class", "x axis")
		.attr("transform", "translate(0," + height + ")")
		.call(xAxis)
		.selectAll("text")  
		.style("text-anchor", "end")
		.attr("dx", "-.8em")
		.attr("dy", ".15em")
		.attr("transform", function(d) {
			return "rotate(-55)" 
		});

    svg.append("g")
		.attr("class", "y axis")
		.call(yAxis)
		.append("text")
		.attr("transform", "rotate(-90)")
		.attr("y", -50)
		.attr("x", 20)
		.style("text-anchor", "end")
		.text("Value");
	  
	var fields = svg.selectAll(".field")
		.data(data);
			  
	var field = fields.enter().append("g")
		.attr("class", "field")
		.attr("id", function(d) { return d.name; });
	 
	var brush = d3.svg.brush()
      .x(x)
      .y(y)
      .on("brushend", brushend);
	 
	 field.append("path")
		.attr("class", "line")
		.attr("d", function(d) { return line(d.values); })
		.style({'fill': 'none','stroke-width':' 3px'})
		.style("stroke", function(d) { return color(d.name); });
	
	var legend = svg.selectAll(".legend")
		.data(color.domain().slice().reverse())
		.enter().append("g")
		.attr("class", "legend")
		.attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

	legend.append("rect")
		.attr("x", 0)
		.attr("y", 400)
		.attr("width", 18)
		.attr("height", 18)
		.style("fill", color);

	legend.append("text")
		.attr("x", 20)
		.attr("y", 414)
		.style("text-anchor", "start")
		.text(function(d) { return d; });
	
	fields.call(brush);		

	function brushend(d) {
		var e = brush.extent();
		var x1=e[0][0];
		var x2=e[1][0];
		$('#plotContainer').html('');
		var newData = [];
		for (var i=0;i<data.length;i++){
			newData[i] = {name: data[i].name, values: []};
			var cnt=0;
			for (var j=0;j<data[i].values.length;j++){
				if (x1<=data[i].values[j].date && x2>=data[i].values[j].date){
					newData[i].values[cnt++] = data[i].values[j]
				}
			}
		}
		drawChartTimeseries(newData);
	  }
}