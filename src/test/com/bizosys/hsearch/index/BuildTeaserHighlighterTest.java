/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.index;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.TestAll;
import com.bizosys.hsearch.filter.TeaserFilterCommon;

public class BuildTeaserHighlighterTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];  
	
	public static void main(String[] args) throws Exception {
		BuildTeaserHighlighterTest t = new BuildTeaserHighlighterTest();
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testRandom(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.testNoContent();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
	}
	
	@Override
	protected void tearDown() throws Exception {
	}
	
	public void testNoContent() throws Exception {
		byte[][] wordL = new byte[][]{"abinash".getBytes()};
		byte[] content = null;
		
		int offsetContentLength =  24;
		TeaserFilterCommon bth = new TeaserFilterCommon(content, 10, offsetContentLength,wordL);
		byte[] teaser = bth.find(45);
		System.out.println(new String(teaser));
		System.out.println("testOffset OK");
	}	
	
	public void testOffset() throws Exception {
		byte[][] wordL = new byte[][]{"abinash".getBytes(), "karan".getBytes()};
		byte[] content = ("Junk Part- Abinash Karan is a software Architect  Shengalipuram, a.k.a Svayam-kalihara-puram, Siva-Kali-puram. " +
			"This is the village of the great Upanyasa Chakravarti, Anantharama Deekshitar, and we" +
			" are on our way to the house where his grandfather, the great sage Muthannavaal lived…" +
			"Here is the street where Muthannavaal’s house is…Muthannavaal () was bo").getBytes();
		
		int offsetContentLength =  24;
		TeaserFilterCommon bth = new TeaserFilterCommon(content, 10, offsetContentLength,wordL);
		byte[] teaser = bth.find(45);
		assertTrue(teaser.length < offsetContentLength);
		assertEquals(" Abinash Karan is a", new String(teaser));
		System.out.println("testOffset OK");
	}
	
	public void testAtBeginning() throws Exception {
		byte[][] wordL = new byte[][]{"hsearch".getBytes()};
		byte[] content = ("HSearch is a distributed open sourced product.").getBytes();
		TeaserFilterCommon bth = new TeaserFilterCommon(content, 0,content.length,wordL);
		byte[] teaser = bth.find(30);
		System.out.println(new String(teaser));
		assertEquals("HSearch is a distributed open", new String(teaser));
		System.out.println("testAtBeginning OK");
	}
	
	public void testAtEnd() throws Exception {
		byte[][] wordL = new byte[][]{"product".getBytes()};
		byte[] content = ("HSearch is a distributed open sourced product.").getBytes();
		TeaserFilterCommon bth = new TeaserFilterCommon(content,0,content.length,wordL);
		byte[] teaser = bth.find(100);
		assertEquals(" is a distributed open sourced product", new String(teaser));
		System.out.println("testAtEnd OK");
	}	
	
	public void testNonExisting() throws Exception {
		byte[][] wordL = new byte[][]{"opens".getBytes()};
		byte[] content = ("HSearch is a distributed open sourced product.").getBytes();
		TeaserFilterCommon bth = new TeaserFilterCommon(content,0,content.length,wordL);
		byte[] teaser = bth.find(10);
		System.out.println(new String(teaser));
		assertEquals(" is a distributed open sourced product", new String(teaser));
		System.out.println("testAtEnd OK");
	}	

	public void testBestPosition() throws Exception {
		byte[][] wordL = new byte[][]{
			"muthannavaal".getBytes(), 
			"claustrophobia".getBytes(), 
			"banyan".getBytes(), "tree".getBytes()};
		
		byte[] content = sampleText.getBytes();
		TeaserFilterCommon bth = new TeaserFilterCommon(content,wordL);
		
		String teaser = new String(bth.find(180));
		System.out.println(teaser);
		//assertEquals(teaser, " brief narration of the life of Muthannavaal, a great Vaidika, a Banyan tree of Dharma. I have taken the details from the brief biography penned by Anantarama Deekshitar in his Jaya");
		System.out.println("testBestPosition OK");
	}
		
	
	private static String sampleText = "We are at Shengalipuram, a.k.a Svayam-kalihara-puram, Siva-Kali-puram. " +
			"This is the village of the great Upanyasa Chakravarti, Anantharama Deekshitar, and we" +
			" are on our way to the house where his grandfather, the great sage Muthannavaal lived…" +
"Here is the street where Muthannavaal’s house is…Muthannavaal () was born sometime around 1830. " +
"Named as Vaidyanatha, he was initiated into Vedic learning at the age of five, when his Upanayanam" +
" was performed. Receiving traditional learning from his father, he completed the study of Veda, Kavya, " +
"Shrouta, Apastambha gruhya sootra, Dharma sutra by the age of 12." + 
"Thereafter, he went on to learn Tarka Shastra from Shyama Shastri, who stayed in Injikollai village nearby. " +
"Walking to his Guru’s house every evening, he would learn Tarka Shastra at night. Staying over, he would" +
" complete his daily rituals (anushtaana) in the morning, and continue his studies, and then return to his " +
"village. That was his daily routine. Ok… Lets step into the house where Sri Muthannavaal lived.Here is a" +
" picture…. This is a traditional Tanjore home…None of the urban claustrophobia here… Lots of sunlight…. " +
"Peacock sounds… Peace… **At the age of 16, Vaidyanatha (Muthannavaal) started Somayaga. Born in a family " +
"that had been performing Agnihotra for many-many generations, he started Nitya Agnihotra – daily Vedic fire " +
"yajna ritual, which he performed for the whole of his life." + 
"Here is a picture of the room where he performed his daily Agnihotra….After commencing Somayaga, Sri Vaidyanatha Deekshita went on to learn Vedanta from a Sanyasi, a Brahmanishtta, Sri Narayanananda who stayed in Kanjanur. Incidentally, Kanjanur is considered especially sacred as the Kaveri rivers flows northward there…." + 
"Sri Muthanna’s daily routine in the morning included bathing before dawn, daily Agnihotra, Vedanta deliberation and teaching, Mahabharata pravacana, Ashwatta pradakshina (circumambulating the Peepal tree), Brahmayajna prashna etc. Afternoon, parayana and teaching of Ramayana, Bhagavata.  In his home, food was offered to all his students and to Sanyasis, everyday. He and his brother Subburama Deekshitar, taught Veda, Vedanta, Shastra, Shrouta, Kavya. Nataka…" + 
"Sri Muthanna’s piety was awesome. When Sri Muthanna did Pancayatana Puja, he would wear a thousand Rudraksha. His daily Mantra japa would include Mantra of Ganapathi, Subrahmanya, Siva Pancakshari, Meda Dakshinamurthi, Shoolini.. He was a Mantra siddha. By the power of his Mantra shakti, he attained Vaaksiddhi, the power of Truth. By that shakti he could heal and bring solace to countless people." + 
"***I am here at his house. His great-great grand nephew Sri Mohanarama Deekshitar has arranged this visit for me. Sri Mohanarama Deekshitar is a renowned Upanyaasa-kaaraa himself, and is deeply learned in Ramayana, Bhagavata and other puranas." + 
"He has told me to make sure that I do some Japa sitting in the Rezhi-thinnai of Muthanna’s house. A rezhi-thinnai is a small platform, a resting place that you see as soon as you enter the covered portion of a house.  In Muthannavaal’s home, it is just big enough for one man to lie down. Sri Mohanarama tells me that Muthanna would do japa of Rama Nama 100,000 times every night in this Rezhi-thinnai, before he retired to sleep." + 
"Here is a picture of that Rezhi.That was Sri Muthanna’s home. Right behind it is a temple of Sri Ranganatha Perumal. Kanchi Paramacharya is supposed to have mentioned that Sri Muthannavaal’s family’s spiritual greatness was a blessing of Sri Ranganatha, for Sri Ranganatha’s feet was pointing towards the house of Muthannavaal." + 
"**Sri Muthanna and his brother imparted Vedic knowledge to several hundreds  of students. There is an interesting story that concerns one his students, Paruthiyur Krishna Shastri. After completing his studies, Sri Krishna Shastri was offered a Government job to teach Vedanta, by the Trivandrum Maharaja’s government. When he conveyed this to his Guru Muthannavaal, he was asked to decline the offer. Sri Muthannavaal told him -  “The Vedanta that you have learnt from me is not to be taught for money”. Sri Krishna Shastri then asked him how he was to make a living. Sri Muthanna told him to take up Ramayana Pravacana, starting that very day in his presence. “You will attain all prosperity (kshema) by narrating Ramayana”, he said. And so it came to be. Sri Krishna Shastri became one of the greatest exponents of Ramayana, and his pravacana was held all over India…" + 
"**In the year Nandana (1893), on the 2nd day of Tamil month of Tai, it was Kaanum pongal day. When his wife had gone to the pond nearby to keep Kanu pidi, Sri Muthanna took Aapat Sanyaasaa. Such a Sanyasa is taken when a person realizes that his life is in danger, and he takes a spot decision to formally renounce worldly life. On Ekadashi of the next fortnight, the dark fortnight of the month of Tai, he told one of his close disciples that he would be giving up his body the next day. And just as he said, he breathed his last on the Dwadashi, Krishna Paksha, month of Tai." + 
"There is another interesting episode that concerns his disciple Krishna Shastri. Deeply beholden to his Guru, one of Sri Krishna Shastri’s heartfelt desire was that he should breathe his last on the same thithi (calender day) as his Guru. And this came to be. He had Kapala-moksha and breathed his last also on Dwadashi, Krishna Paksha, month of Tai, in 1911." + 
"That’s a brief narration of the life of Muthannavaal, a great Vaidika, a Banyan tree of Dharma. I have taken the details from the brief biography penned by Anantarama Deekshitar in his Jaya Mangala Stotram book." + 
"Prostrations to Sri Muthannaavaal!" + 
"Concluding this post with one more picture of his Samadhi shrine…";
}
