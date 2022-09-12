package com.jacobr4d.crawler.utils;

/**
 * Helper class that shows how to parse URLs to obtain host name, port number
 * and file path
 */
public class URLInfo {
	
    private String hostName;
    private int portNo;
    private String filePath;
    private boolean isSecure;

    /**
     * Constructor called with raw URL as input.
     */
    public URLInfo(String docURL) {
        if (docURL == null)
            throw new RuntimeException("docURL is null");
        if (docURL.indexOf("?") > 0)
            docURL = docURL.substring(0, docURL.indexOf("?"));
        if (docURL.isEmpty())
            throw new RuntimeException("docURL is blank");
        if (docURL.startsWith("http://")) {
            isSecure = false;
            docURL = docURL.substring(7);
        } else if (docURL.startsWith("https://")) {
            isSecure = true;
            docURL = docURL.substring(8);
        } else {
            throw new RuntimeException("docURL isn't http or https");
        }
        int i = docURL.indexOf("/");
        filePath = (i < 0) ? "/" : docURL.substring(i);
        int j = filePath.indexOf("#");
        filePath = (j < 0) ? filePath : filePath.substring(0, j);

        String address = (i < 0) ? docURL : docURL.substring(0, i);
        if (address.isBlank())
            throw new RuntimeException("docURL has no address");
        int k = address.indexOf(":");
        if (k < 0) {
            hostName = address;
            portNo = isSecure ? 443 : 80;
        } else {
            hostName = address.substring(0, k).toLowerCase();
            try {
                portNo = Integer.parseInt(address.substring(k + 1));
            } catch (NumberFormatException nfe) {
                portNo = isSecure ? 443 : 80;
                throw new RuntimeException("docURL port could not be parsed");
            }
        }
        // System.out.println(hostName + " " + portNo + " " + filePath + " " + isSecure);
    }
    
    @Override
    public int hashCode() {
		return hostName.hashCode();
    	
    }

    public URLInfo(String hostName, String filePath) {
        this.hostName = hostName;
        this.filePath = filePath;
        this.portNo = 80;
    }

    public URLInfo(String hostName, int portNo, String filePath) {
        this.hostName = hostName;
        this.portNo = portNo;
        this.filePath = filePath;
    }
    
    public URLInfo(String hostName, int portNo, String filePath, boolean isSecure) {
        this.hostName = hostName;
        this.portNo = portNo;
        this.filePath = filePath;
        this.isSecure = isSecure;
    }
    
    public URLInfo copy() {
    	return new URLInfo(hostName, portNo, filePath, isSecure);
    }

    public String getHostName() {
        return hostName;
    }

    public URLInfo setHostName(String s) {
        hostName = s;
        return this;
    }

    public int getPortNo() {
        return portNo;
    }

    public void setPortNo(int p) {
        portNo = p;
    }

    public String getFilePath() {
        return filePath;
    }

    public URLInfo setFilePath(String fp) {
        this.filePath = fp;
        return this;
    }

    public boolean isSecure() {
        return isSecure;
    }

    public void setSecure(boolean sec) {
        isSecure = sec;
    }

    public String toString() {
        return (isSecure ? "https://" : "http://") + hostName + ":" + portNo + filePath;
    }
}
