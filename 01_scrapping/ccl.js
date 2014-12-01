// node ccl.js 1991

// Imports
var sandcrawler = require('sandcrawler'),
	logger = require('sandcrawler-logger'),
	fs = require('fs'),
	process = require('process'),
	MongoClient = require('mongodb').MongoClient,
	format = require('util').format;

// Set limit
var limit = 0;

// Get command line args
var year = process.argv[2];

// Set start url according to year
var startUrls = {
	'1991' : 'http://www.ccl.net/cgi-bin/ccl/message-new?1991+01+11+001',
	'1992' : 'http://www.ccl.net/cgi-bin/ccl/message-new?1992+01+01+001',
	'1993' : 'http://www.ccl.net/cgi-bin/ccl/message-new?1993+01+01+001',
	'1994' : 'http://www.ccl.net/cgi-bin/ccl/message-new?1994+01+02+001',
	'1995' : 'http://www.ccl.net/cgi-bin/ccl/message-new?1995+01+01+001',
	'1996' : 'http://www.ccl.net/cgi-bin/ccl/message-new?1996+01+02+001',
	'1997' : 'http://www.ccl.net/cgi-bin/ccl/message-new?1997+01+01+001',
	'1998' : 'http://www.ccl.net/cgi-bin/ccl/message-new?1998+01+05+001',
	'1999' : 'http://www.ccl.net/cgi-bin/ccl/message-new?1999+01+02+001',
	'2000' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2000+01+01+001',
	'2001' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2001+01+01+001',
	'2002' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2002+01+01+001',
	'2003' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2003+01+02+001',
	'2004' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2004+01+02+001',
	'2005' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2005+01+01+001',
	'2006' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2006+01+01+001',
	'2007' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2007+01+01+001',
	'2008' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2008+01+01+001',
	'2009' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2009+01+01+001',
	'2010' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2010+01+01+001',
	'2011' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2011+01+01+001',
	'2012' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2012+01+01+001',
	'2013' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2013+01+01+001',
	'2014' : 'http://www.ccl.net/cgi-bin/ccl/message-new?2014+01+01+001'
};

MongoClient.connect('mongodb://127.0.0.1:27017/ccl', function(err, db) {
	if(err) throw err;
	var mailsCollection = db.collection('mails');

	// Create and config scraper
	var scraper = new sandcrawler.scraper('ccl')
		.use(logger())
		.config({autoRetry : true, maxRetries : 5, timeout : 20 * 1000})
		.url(startUrls[year])
		.limit(limit)
		.beforeScraping(function(req, next) {
			setTimeout(next, 100);
		})
		.iterate(function(i, req, res) {
			// Filter on year "year" as command line arg and not last email reached
			var reg = new RegExp("\\?" + year + "\\+");
			if((res.data.nav.next.match(reg)) && ((res.data.nav.previous != res.data.nav.next) || (i == 1))) {
				return 'http://www.ccl.net' + res.data.nav.next;
			} else {
				return false;
			}
		})
		.script('./scraper.js')
		.afterScraping(function(req, res, next) {
			mailsCollection.insert(res.data.mail, function(err, docs) {
				if(err) {
					return next(new Error('mongo-error'));
				} else {
					next();
				}
			});
		})
		.on('job:done', function(job) {
			if(job.state.failing) {
				MongoClient.connect('mongodb://127.0.0.1:27017/ccl', function(err, db) {
					if(err) throw err;
					var urlsCollection = db.collection('urls');
					urlsCollection.insert({'url' : job.req.url, 'state' : 'fail'}, function(err, docs) {
						if(err) {
							console.log(err);
						}
					});
					db.close();
				});
			}
		})
		.result(function(err, req, res) {
		});

	sandcrawler.run(scraper, function(err, remains) {
		setTimeout(function() { db.close(); }, 1 * 1000);
		console.log(remains.map(function(item) {
			return item.error;
		}));
	});
});

