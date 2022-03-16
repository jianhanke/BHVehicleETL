package com.neuexample.utils

object MathFuncs {

  val socVoltageArray = Array(
    Array(4176, 4118, 4063, 4009, 3958, 3908, 3861, 3816, 3774, 3731, 3693, 3672, 3655, 3641, 3624, 3598, 3567, 3532, 3494, 3440, 3317),
    Array(4177, 4121, 4066, 4012, 3961, 3912, 3864, 3819, 3777, 3734, 3694, 3672, 3656, 3641, 3624, 3601, 3570, 3537, 3498, 3449, 3343),
    Array(4179, 4123, 4069, 4015, 3964, 3915, 3867, 3822, 3780, 3737, 3695, 3673, 3656, 3641, 3625, 3604, 3574, 3541, 3503, 3458, 3370),
    Array(4181, 4125, 4072, 4019, 3967, 3919, 3872, 3827, 3785, 3743, 3700, 3676, 3659, 3644, 3629, 3608, 3581, 3550, 3514, 3470, 3392),
    Array(4183, 4128, 4075, 4022, 3971, 3923, 3876, 3832, 3790, 3748, 3705, 3680, 3662, 3647, 3632, 3613, 3588, 3559, 3524, 3482, 3413),
    Array(4185, 4130, 4077, 4025, 3975, 3927, 3881, 3837, 3796, 3754, 3711, 3684, 3665, 3649, 3635, 3618, 3596, 3568, 3534, 3494, 3435),
    Array(4189, 4134, 4082, 4031, 3981, 3934, 3889, 3846, 3805, 3765, 3721, 3690, 3670, 3654, 3640, 3626, 3609, 3585, 3554, 3517, 3477),
    Array(4189, 4134, 4084, 4033, 3984, 3938, 3893, 3850, 3810, 3770, 3726, 3694, 3672, 3655, 3641, 3627, 3611, 3590, 3562, 3528, 3490),
    Array(4188, 4135, 4085, 4035, 3984, 3938, 3893, 3850, 3810, 3770, 3726, 3694, 3672, 3655, 3641, 3627, 3611, 3590, 3562, 3528, 3490),
    Array(4187, 4136, 4089, 4041, 3994, 3949, 3906, 3865, 3824, 3784, 3741, 3704, 3678, 3659, 3644, 3631, 3618, 3604, 3586, 3560, 3527),
    Array(4188, 4138, 4092, 4046, 4000, 3956, 3914, 3873, 3833, 3793, 3750, 3712, 3685, 3664, 3649, 3635, 3622, 3609, 3593, 3572, 3544),
    Array(4188, 4140, 4096, 4051, 4006, 3963, 3922, 3882, 3842, 3802, 3759, 3721, 3692, 3670, 3653, 3639, 3626, 3614, 3600, 3584, 3561),
    Array(4188, 4140, 4096, 4052, 4009, 3966, 3925, 3885, 3845, 3804, 3762, 3725, 3695, 3673, 3655, 3640, 3627, 3614, 3600, 3584, 3563),
    Array(4188, 4140, 4097, 4054, 4011, 3970, 3929, 3889, 3848, 3805, 3764, 3728, 3699, 3676, 3657, 3642, 3628, 3614, 3600, 3584, 3565),
    Array(4187, 4140, 4099, 4058, 4017, 3976, 3936, 3897, 3856, 3815, 3775, 3740, 3710, 3686, 3666, 3649, 3633, 3619, 3605, 3589, 3572),
    Array(4187, 4140, 4102, 4063, 4023, 3983, 3944, 3904, 3864, 3824, 3786, 3752, 3722, 3696, 3674, 3656, 3639, 3624, 3609, 3594, 3580),
    Array(4186, 4140, 4102, 4065, 4027, 3988, 3951, 3913, 3876, 3838, 3803, 3769, 3739, 3713, 3689, 3673, 3659, 3638, 3618, 3603, 3591),
    Array(4186, 4140, 4102, 4066, 4030, 3994, 3957, 3922, 3887, 3853, 3819, 3787, 3757, 3729, 3704, 3690, 3679, 3652, 3627, 3613, 3602)
  )
  val socNums = Array(100, 95, 90, 85, 80, 75, 70, 65, 60, 55, 50, 45, 40, 35, 30, 25, 20, 15, 10, 5, 0)


  def main(args: Array[String]): Unit = {

    //val result: Double = calculateSoc(19, 3947)
      //println("result: "+result);
    val socMax: Double = linearInsertion(3984, 80, 3938, 75, 3947)
    // println("socMax："+  socMax)

    var arr = Array(10,1,25,11,24,13,54,4,42,-3);
    var arr2 = Array(1,2,3,4);
    val tuple2: (Double, Double) = calculateQuartile(arr)
    println( tuple2._1 )
    println( tuple2._2 )

  }

  def calculateQuartile(arr: Array[Int])={

    val array: Array[Int] = arr.sortWith(_ < _)
    val x1: Double =  1 * (array.length-1) / 4.0
    val x1IntPart: Int = x1.toInt
    val x1DecPart: Double=x1-x1IntPart;

    val x3: Double= 3 * (array.length-1) / 4.0
    val x3IntPart: Int = x3.toInt
    val x3DecPart: Double = x3-x3IntPart

//    println(x1+"--"+x1IntPart+"--"+x1DecPart+"--"+array(x1IntPart));
//    println(x3+"--"+x3IntPart+"--"+x3DecPart+"--"+array(x3IntPart));

    ((array(x1IntPart + 1) - array(x1IntPart)) * x1DecPart + array(x1IntPart),(array(x3IntPart + 1) - array(x3IntPart)) * x3DecPart + array(x3IntPart))
  }

  def linearInsertion (x0: Double,y0: Double,x1: Double,y1: Double,x: Int ): Double ={

    return (x - x1) / (x0 - x1) * y0 + (x - x0) / (x1 - x0) * y1

  }

  def calculateSoc(temperature: Integer, voltage: Integer): Double ={

    val voltageArray = socVoltageArray(getTemperatureIndex(temperature))
    if(voltage <= voltageArray(voltageArray.length - 1) ){
      return socNums(voltageArray.length - 1);
    }
    if( voltage >= voltageArray(0) ){
      return socNums(0);
    }

    for( i <- 1 until voltageArray.length   ){
      if( voltage >= voltageArray(i)  ){
        return linearInsertion( voltageArray(i - 1),socNums(i - 1),voltageArray(i),socNums(i),voltage  )
      }
    }
    0
  }



  def getTemperatureIndex(temperature: Integer): Integer ={

    if(temperature > 55)
      return 0;
    if(temperature < -30)
      return 17;

    var x=0;
    var i: Int = temperature / 5
    var j: Int = temperature % 5
    if(temperature >= 0){
      if( j<= 2.5 ){
        x = 11 - i;
      }else{
        x= 11 - ( i + 1);
      }
    }else{
      i = Math.abs(i)
      j = Math.abs(j)
      if( j <= 2.5 ){
        x = 11+ i;
      }else{
        x = 11+ i + 1;
      }
    }
    x
  }

}
