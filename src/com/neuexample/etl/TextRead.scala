package com.neuexample.etl

import com.neuexample.streaming.WarningSteaming.properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TextRead {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master(properties.getProperty("spark.master"))
      .appName("SparkStreamingKafkaDirexct")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("D:\\new 1.txt")

//    rdd.foreach(line=>{
//        val strings: Array[String] = line.split("\t")
//      val arr = new Array[Int](strings.length)
//      for(i <- 0 until strings.length){
//        arr(i)= strings(i).toInt
//      }
//       println(arr.mkString("Array(", ",", "),"))
//    })

    rdd.foreach(line=>{
      val strs: Array[String] = line.split(",")
            for(i <- 0 until strs.length) {
              println(" println( \" "+  strs(i)+":" +" \" +" + strs(i)+")"  )
            }

    })


    val socControllerStragegyArray = Array(
      Array(4176,4176,4118,4062,4009,3958,3908,3861,3816,3774,3731,3693,3672,3655,3641,3624,3598,3567,3532,3494,3440,3317),
      Array(4177,4177,4121,4066,4011,3961,3912,3864,3819,3777,3734,3694,3672,3656,3641,3624,3601,3570,3537,3498,3449,3343),
      Array(4179,4179,4123,4069,4014,3964,3915,3867,3822,3780,3737,3695,3673,3656,3641,3625,3604,3574,3541,3503,3458,3370),
      Array(4181,4181,4125,4072,4019,3967,3919,3872,3827,3785,3743,3700,3676,3659,3644,3629,3608,3581,3550,3514,3470,3392),
      Array(4183,4183,4128,4075,4022,3971,3923,3876,3832,3790,3748,3705,3680,3662,3647,3632,3613,3588,3559,3524,3482,3413),
      Array(4185,4185,4130,4077,4025,3975,3927,3881,3837,3796,3754,3711,3684,3665,3649,3635,3618,3596,3568,3534,3494,3435),
      Array(4189,4189,4134,4082,4030,3981,3934,3889,3846,3805,3765,3721,3690,3670,3654,3640,3626,3609,3585,3554,3517,3477),
      Array(4189,4189,4134,4083,4033,3984,3938,3893,3850,3810,3770,3726,3694,3672,3655,3641,3627,3611,3590,3562,3528,3490),
      Array(4188,4188,4135,4085,4035,3984,3938,3893,3850,3810,3770,3726,3694,3672,3655,3641,3627,3611,3590,3562,3528,3490),
      Array(4187,4187,4136,4089,4041,3994,3949,3906,3865,3824,3784,3741,3704,3678,3659,3644,3631,3618,3604,3586,3560,3527),
      Array(4188,4188,4138,4091,4046,4000,3956,3914,3873,3833,3793,3750,3712,3685,3664,3649,3635,3622,3609,3593,3572,3544),
      Array(4188,4188,4140,4096,4051,4006,3963,3922,3882,3842,3802,3759,3721,3692,3670,3653,3639,3626,3614,3600,3584,3561),
      Array(4188,4188,4140,4096,4051,4009,3966,3925,3885,3845,3804,3762,3725,3695,3673,3655,3640,3627,3614,3600,3584,3563),
      Array(4188,4188,4140,4097,4054,4011,3970,3929,3889,3848,3805,3764,3728,3699,3676,3657,3642,3628,3614,3600,3584,3565),
      Array(4187,4187,4140,4099,4058,4017,3976,3936,3897,3856,3815,3775,3740,3710,3686,3666,3649,3633,3619,3605,3589,3572),
      Array(4187,4187,4140,4102,4062,4022,3983,3944,3904,3864,3824,3786,3752,3722,3696,3674,3656,3639,3624,3609,3594,3580),
      Array(4186,4186,4140,4102,4065,4027,3988,3951,3913,3876,3838,3803,3769,3739,3713,3689,3673,3659,3638,3618,3603,3591),
      Array(4186,4186,4140,4102,4066,4030,3994,3957,3922,3887,3853,3819,3787,3757,3729,3704,3690,3679,3652,3627,3613,3602)
    )

    println(socControllerStragegyArray.length)
    println(socControllerStragegyArray(0).length);
    println(socControllerStragegyArray(1).length);
    for( i <- 0 until socControllerStragegyArray.length){
      for( j <- 0 until socControllerStragegyArray(i).length ){
         print(socControllerStragegyArray(i)(j)+" ")
      }
      println()
    }
    println("kaishi")
    val arrays: Array[String] = new Array[String](22)
    println("changdu:"+arrays.length)
    for( i <- 1 until arrays.length ){
      arrays(i)=  ((arrays.length-1-i) * 0.05).formatted("%.2f")
    }
    arrays(0)=arrays(1);
    println (   arrays.mkString("Array(", ",", "),") )

    val socNums = Array(1.00,1.00,0.95,0.90,0.85,0.80,0.75,0.70,0.65,0.60,0.55,0.50,0.45,0.40,0.35,0.30,0.25,0.20,0.15,0.10,0.05,0.00)
    println(socNums.length)


    sc.stop()
  }

}
