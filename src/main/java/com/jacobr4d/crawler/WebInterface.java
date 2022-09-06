package com.jacobr4d.crawler;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import com.sleepycat.persist.EntityCursor;

import spark.Request;
import spark.Response;
import spark.Session;
import spark.Spark;

import com.jacobr4d.crawler.utils.DateUtils;
import com.jacobr4d.crawler.utils.HttpUtils;
import com.jacobr4d.crawler.utils.URLInfo;
import com.jacobr4d.indexer.index.Index;
import com.jacobr4d.indexer.index.InvertedHit;
import com.jacobr4d.crawler.repository.*;


public class WebInterface {
	
	public static int SESSION_TIMEOUT_MINUTES = 5;
	
	String dbPath;
	String staticFilesPath;
	Repository db;
	Index index;
	
	public WebInterface(String dbPath, String staticFilesPath) {
		this.dbPath = dbPath;
		this.staticFilesPath = staticFilesPath;
		db = new Repository(dbPath);
		this.index = new Index("output/index");
		
		/* set up spark server */
		Spark.staticFiles.externalLocation(staticFilesPath);
		Spark.port(45555);
		// registerLoginFilter();
		// registerRegister();
		// registerLogin();
		// registerLogout();
		registerHomePage();
		registerLookup();
		// registerCreateChannel();
		// registerShowChannelPage();
		registerShutdown();
		// registerData();
		Spark.awaitInitialization();
	}
	
	public void shutdown() {
		db.close();
		index.close();
	}
	
	String homePageHtml(Request req, Response res) {
		StringBuilder html = new StringBuilder();
		html.append("<!DOCTYPE html><html>");
		html.append("<head>");
		html.append("<script>");
    	html.append("function createFun(){");
    	html.append("var action_src = \"/create/\" + ");
    	html.append("document.getElementsByName(\"channelName\")[0].value + ");
    	html.append("\"?xpath=\" + document.getElementsByName(\"xpath\")[0].value;");
		html.append("var your_form = document.getElementById('createChannel');");
		html.append("your_form.action = action_src;");
		html.append("}");
		html.append("</script>");
		html.append("</head>");
		html.append("<body>");

		html.append("<h1>Welcome Admin</h1>");

		html.append("<h2>Lookup Document (BROKEN)</h2>");
    	html.append("<form action=\"./lookup\" method=\"get\">");
    	html.append("URL <input type=\"text\" name=\"url\" />");
    	html.append("<button type=\"submit\" class=\"btn-link\">Lookup</button>");
    	html.append("</form>");
    
		html.append("<h2>Documents</h2>");
		html.append("<ul>");
		EntityCursor<String> documentCursor = db.documentCursor();
		try {
			for (String url = documentCursor.first(); url != null; url = documentCursor.next()) {
				html.append("<li>");
	    		html.append("<a href=\"/lookup?url=" + HttpUtils.encodeURL(url) + "\">" + url + "</a>");
	    		html.append("</li>");
			}
		} finally {
			documentCursor.close();
		}
		html.append("</ul>");
		
		html.append("<h2>InvertedIndex</h2>");
		html.append("<ul>");
		EntityCursor<String> words = index.words();
		try {
			for (String word = words.first(); word != null; word = words.nextNoDup()) {
				EntityCursor<InvertedHit> hits = index.invertedHitsofWord(word);
				try {
					for (InvertedHit hit = hits.first(); hit != null; hit = hits.next()) {
						html.append("<li>");
			    		html.append(word + " " + hit.url);
			    		html.append("</li>");
					}
				} finally {
					hits.close();
				}
			}
		} finally {
			words.close();
		}
		html.append("</ul>");
		
		html.append("<h2>Shutdown</h2>");
    	html.append("<form action=\"./shutdown\" method=\"get\">");
    	html.append("<button type=\"submit\" class=\"btn-link\">Shutdown</button>");
    	html.append("</form>");
    	html.append("</body></html>");
    	return html.toString();
	}
	
	void registerHomePage() {
		Spark.get("/", (req, res) -> {
			return homePageHtml(req, res);
		});
		Spark.get("/index.html", (req, res) -> {
			return homePageHtml(req, res);
		});
	}
	/* */
	static void registerLoginFilter() {
		Spark.before((req, res) -> {
			List<String> anon = Arrays.asList("/login-form.html", "/register.html", "/lookup", "/login", "/register", "/data", "/show");
			if (!anon.contains(req.pathInfo())) {
	            if (req.session(false) == null) {
	                res.redirect("/login-form.html");
	                Spark.halt(400);
	            } else {
	                req.attribute("username", req.session().attribute("username"));
	            }
	        }
		});
	}
	
	void registerRegister() {
		Spark.post("/register", (req, res) -> {
			String user = req.queryParams("username");
	        String pass = req.queryParams("password");
	        	        	        
	        /* clear session if it exists */
			if (req.session(false) != null) {
	        	req.session().invalidate();
	        }
			
			if (user.isBlank()) {
				Spark.halt(400, "Username blank");
			} else if (pass.isBlank()) {
				Spark.halt(400, "Password blank");
			} else if (db.getUser(user) != null) {
				Spark.halt(400, "Username taken");
	        } else {
	        	db.putUser(user, pass);
	            res.redirect("/");
	        }
			return "User registered";
		});
	}
	
	void registerData() {
		Spark.get("/data", (req, res) -> {
			return "Numchannels = " + db.getNumChannels() + "\nNumMatches = " + db.getNumMatches();
		});
	}
	
	void registerLogin() {
		Spark.post("/login", (req, res) -> {
			String user = req.queryParams("username");
	        String pass = req.queryParams("password");
	        
	        /* clear session if it exists */
	        if (req.session(false) != null) {
	        	req.session().invalidate();
	        }
	        
	    	if (user == null || pass == null || !db.validate(user, pass)) {
	            Spark.halt(400, "Invalid credentials");
	        } else {
	            Session session = req.session();
	            session.maxInactiveInterval(SESSION_TIMEOUT_MINUTES * 60);
	            session.attribute("username", user);
	            session.attribute("password", pass);
	            res.redirect("/");
	        }
	        return "Logged in";
		});
	}
	
	static void registerLogout() {
		Spark.post("/logout", (req, res) -> {
			if (req.session(false) == null) {
				Spark.halt(400, "Not logged in");
	        } else {
				req.session().invalidate();
	        }
			return "Logged out";
		});
	}
	
	void registerLookup() {
		Spark.get("/lookup", (req, res) -> {
			String url = req.queryParams("url");
			Document document = db.getDocument(new URLInfo(HttpUtils.decodeURL(url)));
			if (document == null)
				Spark.halt(404, "Document not found");
			res.type(document.contentType);
			return document.raw;
		});
	}
	
	void registerCreateChannel() {
		Spark.get("/create/:name", (req, res) -> {
			String name = req.params("name");
			String xpath = req.queryParams("xpath");
			String username = req.attribute("username");
			
			if (name == null)
				Spark.halt(400, "Name not provided");
			if (xpath == null)
				Spark.halt(400, "Xpath not provided");
			if (!HttpUtils.isValidChannelName(name))
				Spark.halt(400, "Name is invalid");
			if (!HttpUtils.isValidXPath(xpath))
				Spark.halt(400, "Xpath is invalid");
			if (db.getChannel(name) != null)
				Spark.halt(400, "Channel name taken");
			
			Channel channel = new Channel();
			channel.channelName = name;
			channel.pattern = xpath;
			channel.creator = username;
			db.putChannel(channel);
			return "Success";
		});
	}
	
	void registerShowChannelPage() {
		Spark.get("/show", (req, res) -> {
			String channelName = req.queryParams("channel");
			
			Channel channel = db.getChannel(channelName);
			if (channel == null)
				Spark.halt(404, "Channel not found");
						
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			StringBuilder html = new StringBuilder();
			html.append("<html>");
			html.append("<body>");
			html.append("<div class=\"headerDiv\" style=\"background-color: Gray; margin: 5; padding-top: 10; padding-bottom: 10\" >");
			html.append("<div class=\"channelheader\">");
			html.append("<h2>Channel name: " + channelName + ", xpath: " + channel.pattern + ", created by: " + channel.creator + "</h2>");
			html.append("</div>");
			html.append("</div>");
			html.append("<div class=\"documentItems\" >");
			out.write(html.toString().getBytes());
			
			EntityCursor<DocumentMatchesChannel> matchCursor = db.documentMatchesChannelCursor(channelName);
			try {
				for (DocumentMatchesChannel match = matchCursor.first(); match != null; match = matchCursor.next()) {
					
					Document doc = db.getDocument(new URLInfo(match.documentURL));
					if (doc == null)
						Spark.halt(404, "Document not found");
					
					out.write("<div class=\"channelItem\" style=\"border-style: dashed; border-width: 1; margin: 5\" >".getBytes());
					out.write("<div class=\"meta\" style=\"border-style: dashed; border-width: 1; background-color:LightGray;\" >".getBytes());
					out.write(("<h4>Crawled on: " + DateUtils.showDate(doc.crawledTime) + "</h4>").getBytes());
					out.write(("<h4>Location: " + doc.url + "</h4>").getBytes());
					out.write("</div>".getBytes());
					
					out.write("<div class=\"document\">".getBytes());
					out.write(doc.raw);
					out.write("</div>".getBytes());

					out.write("</div>".getBytes());
					
    			}
			} finally {
				matchCursor.close();
			}

    		
			StringBuilder html2 = new StringBuilder();
			html2.append("</div>");
			html2.append("</body>");
			html2.append("</html>");
			out.write(html2.toString().getBytes());
			return out.toByteArray();
		});
	}
	
	void registerShutdown() {
		Spark.get("/shutdown", (req, res) -> {
			System.out.println("Shutting down...");
			shutdown();
			HttpUtils.exitInOneSecond();
			return "Shutting down";
		});
	}
	
    public static void main(String args[]) throws IOException {
    	
        if (args.length < 1 || args.length > 2) {
            System.out.println("Syntax: WebInterface {dbpath} {staticfilesrootdir}");
            System.exit(1);
        }

        WebInterface webInterface = new WebInterface(args[0], args[1]);
       
        System.out.println("Press [Enter] to shut down...");
		(new BufferedReader(new InputStreamReader(System.in))).readLine();
		
		webInterface.shutdown();
		
		System.exit(0);

    }
}
