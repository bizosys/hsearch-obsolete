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

package com.bizosys.hsearch.dictionary;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 
 * W. Burkhard and R. Keller. Some approaches to best-match file searching,
 * CACM, 1973
 * 
 * http://portal.acm.org/citation.cfm?doid=362003.362025
 * 
 * 
 * From Wikipedia, the free encyclopedia:
 * 
 * A BK-tree is a metric tree suggested by Burkhard and Keller specifically
 * adapted to discrete metric spaces.
 * 
 * For simplicity, let us consider integer discrete metric p(x,y). Then, BK-tree
 * is defined in the following way. An arbitrary element a is selected as root
 * node. Root node may have zero or more subtrees. The k-th subtree is
 * recursively built of all elements b such that p(a,b)=k. BK-trees can be used
 * for approximate string matching in a dictionary.
 * 
 * http://en.wikipedia.org/wiki/BK-tree
 * http://blog.notdot.net/2007/4/Damn-Cool-Algorithms-Part-1-BK-Trees
 * 
 * To further improve performance, implement (fast) Map interface for query().
 * Don't forget about fast similarity!
 * 
 * http://www.dcs.shef.ac.uk/~sam/stringmetrics.html - huge list of different
 * metrics
 * 
 * http://www.catalysoft.com/articles/StrikeAMatch.html - extremely simple algo
 * providing fast results (better than Levenstein)
 * 
 * @author Fuad Efendi
 * 
 */
public class BKTree<E> {

	private Node root;
	private Distance distance;

	public BKTree(Distance distance) {
		root = null;
		this.distance = distance;
	}

	public void add(E term) {
		if (root != null) {
			root.add(term);
		} else {
			root = new Node(term);
		}
	}

	public Map<E, Integer> query(E searchObject, int threshold) {
		Map<E, Integer> matches = new HashMap<E, Integer>();
		root.query(searchObject, threshold, matches);
		return matches;
	}

	private class Node {

		E term;
		Map<Integer, Node> children;

		public Node(E term) {
			this.term = term;
			children = new TreeMap<Integer, Node>();
		}

		public void add(E term) {
			int score = distance.getDistance(term, this.term);
			Node child = children.get(score);
			if (child != null) {
				child.add(term);
			} else {
				children.put(score, new Node(term));
			}
		}

		public void query(E term, int threshold, Map<E, Integer> collected) {
			int distanceAtNode = distance.getDistance(term, this.term);
			if (distanceAtNode <= threshold) {
				collected.put(this.term, distanceAtNode);
			}
			for (int score = distanceAtNode - threshold; score <= distanceAtNode + threshold; score++) {
				if (score > 0) {
					Node child = children.get(score);
					if (child != null) {
						child.query(term, threshold, collected);
					}
				}
			}
		}
	}

}
