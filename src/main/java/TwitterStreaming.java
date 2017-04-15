import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import twitter4j.GeoLocation;
import twitter4j.Status;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
public class TwitterStreaming {
	public static void main(String[] args) {
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);

		final String consumerKey = "HolMAvLWP9JAEQZVGQnL9Pb8O";
		final String consumerSecret = "aW9OmWkzvn22om2zqesfFfX46jC6JGb323UCm8MqDORRHUBzmx";
		final String accessToken = "622149293-qi0tgOq8GSE7Zf9tbfQesgso6GP6Vgwhmyrw7H89";
		final String accessTokenSecret = "e5rHzCrYgj3tlMG4L0ikdZZaMXGJn9JXHrrkGdXCMwvM8";

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TwitterStreamingWithMyOptions");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));

		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

		JavaReceiverInputDStream<Status> streamingNews = TwitterUtils.createStream(jssc);
		JavaDStream<Status> withGeolocation = streamingNews.filter(new Function<Status, Boolean>(){
			public Boolean call (Status tweet){
				if (tweet.getGeoLocation()!=null){  
					return true;}
			    else {
				    return false;}}});
		
		JavaDStream<String> tweets = withGeolocation.map(new Function<Status, String>() {
			public String call(Status tweet) {
				return "User: " + tweet.getUser().getName() + "   Lang:" + tweet.getUser().getLang() + "   Friends: "
						+ tweet.getUser().getFriendsCount() + "  " + tweet.getGeoLocation() 
						+ " \nTweet: " + tweet.getText() + "\n";
			}

		});
		tweets.dstream().saveAsTextFiles("TweetsInTextFiles/result", "txt");
		tweets.print();
		jssc.start();
	}
}
