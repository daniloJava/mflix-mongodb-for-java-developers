package mflix.api.daos;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;

import mflix.api.models.Comment;
import mflix.api.models.Critic;

@Component
public class CommentDao extends AbstractMFlixDao {

	public static String COMMENT_COLLECTION = "comments";

	private MongoCollection<Comment> commentCollection;

	private CodecRegistry pojoCodecRegistry;

	private final Logger log;

	@Autowired
	public CommentDao(MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
		super(mongoClient, databaseName);
		log = LoggerFactory.getLogger(this.getClass());
		db = this.mongoClient.getDatabase(MFLIX_DATABASE);
		pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), fromProviders(PojoCodecProvider.builder().automatic(true).build()));
		commentCollection = db.getCollection(COMMENT_COLLECTION, Comment.class).withCodecRegistry(pojoCodecRegistry);
	}

	/**
	 * Returns a Comment object that matches the provided id string.
	 *
	 * @param id
	 *           - comment identifier
	 * @return Comment object corresponding to the identifier value
	 */
	public Comment getComment(String id) {
		return commentCollection.find(new Document("_id", new ObjectId(id))).first();
	}

	/**
	 * Adds a new Comment to the collection. The equivalent instruction in the mongo shell would be:
	 * <p>
	 * db.comments.insertOne({comment})
	 * <p>
	 *
	 * @param comment
	 *                - Comment object.
	 * @throw IncorrectDaoOperation if the insert fails, otherwise returns the resulting Comment object.
	 */
	public Comment addComment(Comment comment) {

		// TODO> Ticket - Update User reviews: implement the functionality that enables adding a new
		// comment.
		// TODO> Ticket - Handling Errors: Implement a try catch block to
		// handle a potential write exception when given a wrong commentId.
		if (comment.getId() == null) {
			throw new IncorrectDaoOperation("comment id cannot be set to null");
		}
		commentCollection.insertOne(comment);
		return comment;
	}

	/**
	 * Updates the comment text matching commentId and user email. This method would be equivalent to running the following
	 * mongo shell command:
	 * <p>
	 * db.comments.update({_id: commentId}, {$set: { "text": text, date: ISODate() }})
	 * <p>
	 *
	 * @param commentId
	 *                  - comment id string value.
	 * @param text
	 *                  - comment text to be updated.
	 * @param email
	 *                  - user email.
	 * @return true if successfully updates the comment text.
	 */
	public boolean updateComment(String commentId, String text, String email) {

		// TODO> Ticket - Update User reviews: implement the functionality that enables updating an
		// user own comments
		Bson filter = Filters.eq("_id", new ObjectId(commentId));
		Comment comment = commentCollection.find(filter).first();
		if (comment != null && !comment.getEmail().equalsIgnoreCase(email)) {
			return false;
		}
		Bson setUpdate = Updates.set("text", text);
		UpdateOptions options = new UpdateOptions().upsert(true);
		commentCollection.updateOne(filter, setUpdate, options);
		// TODO> Ticket - Handling Errors: Implement a try catch block to
		// handle a potential write exception when given a wrong commentId.
		return true;
	}

	/**
	 * Deletes comment that matches user email and commentId.
	 *
	 * @param commentId
	 *                  - commentId string value.
	 * @param email
	 *                  - user email value.
	 * @return true if successful deletes the comment.
	 */
	public boolean deleteComment(String commentId, String email) {
		Bson filter = Filters.and(Filters.eq("email", email), Filters.eq("_id", new ObjectId(commentId)));
		DeleteResult res = commentCollection.deleteOne(filter);
		if (res.getDeletedCount() != 1) {
			log.warn("Not able to delete comment `{}` for user `{}`. User" + " does not own comment or already deleted!", commentId, email);
			return false;
		}
		return true;
	}

	/**
	 * Ticket: User Report - produce a list of users that comment the most in the website. Query the `comments` collection
	 * and group the users by number of comments. The list is limited to up most 20 commenter.
	 *
	 * @return List {@link Critic} objects.
	 */
	public List<Critic> mostActiveCommenters() {
		List<Critic> mostActive = new ArrayList<>();
		// // TODO> Ticket: User Report - execute a command that returns the
		// // list of 20 users, group by number of comments. Don't forget,
		// // this report is expected to be produced with an high durability
		// // guarantee for the returned documents. Once a commenter is in the
		// // top 20 of users, they become a Critic, so mostActive is composed of
		// // Critic objects.
		return mostActive;
	}
}