package org.sagebionetworks.workers.util.aws.message;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

public class TopicUtilsTest {

	@Test (expected=IllegalArgumentException.class)
	public void generateSourceArnWithNull() {
		TopicUtils.generateSourceArn(null);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void generateSourceArnWithEmptyList() {
		TopicUtils.generateSourceArn(new ArrayList<String>());
	}
	
	@Test
	public void generateSourceArnWithValidLists() {
		assertEquals("one", TopicUtils.generateSourceArn(Arrays.asList("one")));
		assertEquals("[\"one\", \"two\"]", TopicUtils.generateSourceArn(Arrays.asList("one","two")));
	}
	
	@Test
	public void containsAllTopicsTest() {
		assertTrue(TopicUtils.containsAllTopics(null, new ArrayList<String>()));
		assertTrue(TopicUtils.containsAllTopics("{one}", Arrays.asList("one")));
		assertTrue(TopicUtils.containsAllTopics("{one,two,three}", Arrays.asList("one","three","two")));
		assertFalse(TopicUtils.containsAllTopics("{one,two}", Arrays.asList("one","three","two")));
	}
}
