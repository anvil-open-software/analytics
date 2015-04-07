package com.dematic.labs.producers.kinesis;

/**
 * Created by silveir on 4/2/15.
 */

import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.chrome.ChromeDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class EventProducerIT {
    final int ORDERS_SIZE = 10;
    final int NODES_SIZE = 5;
    final int SAMPLES_SIZE = 10;
    final int MEAN = 1000*60*6; // six minutes
    final int STD_DEV = 1000*60; // 1 minute

    @Test
    public void test0000GetLandingPage ()  {
        int i;

        class Sample {
            private int order;
            private int node;
            private int milliseconds;

            Sample(int order, int node, int milliseconds) {
                this.order = order;
                this.node = node;
                this.milliseconds = milliseconds;
            }

            public void setOrder(int order) {
                this.order = order;
            }

            public void setNode(int node) {
                this.node = node;
            }

            public void setMilliseconds(int milliseconds) {
                this.milliseconds = milliseconds;
            }

            public int getOrder() {
                return order;
            }

            public int getNode() {
                return node;
            }

            public int getMilliseconds() {
                return milliseconds;
            }
        }
        List<Sample> samples = new ArrayList<Sample>();
        int order;
        int node;
        int milliseconds;
        Random randomGenerator = new Random();

        /*
         Produce a samples array consisting of SAMPLES_SIZE samples build as follows:
         - order: a random number, between 1 and ORDERS_SIZE
         - node: : a random number, between 1 and NODES_SIZE
         - milliseconds: a random Gaussian number with
           - a standard deviation of 1 minutes (1000*60)
           - a mean of 6 minutes (1000*60*6)
         */
        for (i=0; i<SAMPLES_SIZE; i++) {
            order = randomGenerator.nextInt(ORDERS_SIZE) + 1;
            node = randomGenerator.nextInt(NODES_SIZE) + 1;
            do {
                milliseconds = (int)Math.round(randomGenerator.nextGaussian() * STD_DEV + MEAN);
            } while (milliseconds <= 0);
            Sample sample = new Sample(order, node, milliseconds);
            samples.add(sample);
            System.out.println("Sample " + i + ": " + sample.getOrder() + ", " + sample.getNode() + ", " + sample.getMilliseconds());
        }
        assertEquals(0, 0);
    }
}
